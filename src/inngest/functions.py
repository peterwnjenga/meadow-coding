import inngest
import aiohttp
from pprint import pformat
from urllib.parse import quote
import logging
import os
from dotenv import load_dotenv

from .client import inngest_client
from .helpers import summarize_plot_with_openai, send_email_with_resend, generate_movie_email_html

load_dotenv()

logger = logging.getLogger(__name__)

OMDB_API_KEY = os.getenv("OMDB_API_KEY")

@inngest_client.create_function(
    fn_id="movie-plot-summary",
    trigger=inngest.TriggerEvent(event="meadow_api/movie.watched"),
    retries=3,  # Add retries for resilience
)
async def get_movie_plot(
    ctx: inngest.Context,
    step: inngest.Step,
) -> dict:
    # Get movie title from the event data
    movie_title = ctx.event.data.get("movie_title")
    recipient_email = ctx.event.data.get("recipient_email")
    # recipient_email = "peter@reframe.is"
    
    # Check if movie title exists
    if not movie_title:
        logger.error("No movie title provided in the event data")
        raise inngest.NonRetriableError(
            message="No movie title provided in the event data"
        )
    
    # Check if recipient email exists
    if not recipient_email:
        logger.error("No recipient email provided in the event data")
        raise inngest.NonRetriableError(
            message="No recipient email provided in the event data"
        )
    
    # Fetch movie details from OMDB API
    async with aiohttp.ClientSession() as session:
        encoded_title = quote(movie_title)
        url = f"http://www.omdbapi.com/?apikey={OMDB_API_KEY}&t={encoded_title}"
        
        async with session.get(url) as response:
            movie_data = await response.json()
            
            if movie_data.get("Response") == "True":
                # Check if plot exists and is not "N/A"
                plot = movie_data.get("Plot", "")
                if not plot or plot == "N/A":
                    # If there's no plot available return an error, retrying won't help - the data simply isn't there
                    logger.warning(f"No plot found for movie '{movie_title}'")
                    raise inngest.NonRetriableError(
                        message=f"No plot available for movie '{movie_title}'"
                    )
                
                # Log movie data for debugging (consider removing in production)
                logger.info(f"Movie data retrieved: {pformat(movie_data)}")
                
                # Use OpenAI to summarize the plot
                summary = await summarize_plot_with_openai(plot)
                
                # Generate email content using the new function
                email_content = generate_movie_email_html(
                    movie_data=movie_data,
                    plot=plot,
                    summary=summary,
                    movie_title=movie_title
                )
                
                email_subject = f"Movie Summary: {movie_title}"
                email_result = await send_email_with_resend(
                    recipient_email=recipient_email,
                    subject=email_subject,
                    content=email_content,
                    wait_for_status=True
                )
                
                # Check delivery status if available
                delivery_status = "unknown"
                if email_result.get("success") and "delivery_status" in email_result:
                    delivery_status = email_result["delivery_status"].get("final_status", "unknown")
                    logger.info(f"Email delivery status: {delivery_status}")
                
                return {
                    "movie_title": movie_title,
                    "summary": summary,
                    "email_sent": email_result["success"],
                    "email_delivery_status": delivery_status
                }
            else:
                logger.error(f"Movie '{movie_title}' not found in OMDB")
                raise inngest.NonRetriableError(
                    message=f"Movie '{movie_title}' not found in OMDB"
                )