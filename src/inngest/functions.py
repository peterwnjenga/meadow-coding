import inngest
import aiohttp
from pprint import pformat
from urllib.parse import quote
import logging
import os
from dotenv import load_dotenv

from .client import inngest_client
from .helpers import summarize_plot_with_openai, send_email_with_resend, generate_movie_email_html

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# API key for Open Movie Database
OMDB_API_KEY = os.getenv("OMDB_API_KEY")

@inngest_client.create_function(
    fn_id="movie-plot-summary",
    trigger=inngest.TriggerEvent(event="meadow_api/movie.watched"),
    retries=3,  # Retry mechanism for resilience
)
async def get_movie_plot(
    ctx: inngest.Context,
    step: inngest.Step,
) -> dict:
    """
    Fetches movie details, summarizes the plot, and emails the summary to a recipient.
    
    Args:
        ctx: Inngest context containing event data
        step: Inngest step for function execution
        
    Returns:
        dict: Result containing movie title, summary, and email status
        
    Raises:
        inngest.NonRetriableError: For issues that won't be resolved by retrying
    """
    # Extract data from the event
    movie_title = ctx.event.data.get("movie_title")
    recipient_email = ctx.event.data.get("recipient_email")
    
    # Validate required inputs
    if not movie_title:
        logger.error("No movie title provided in the event data")
        raise inngest.NonRetriableError(
            message="No movie title provided in the event data"
        )
    
    if not recipient_email:
        logger.error("No recipient email provided in the event data")
        raise inngest.NonRetriableError(
            message="No recipient email provided in the event data"
        )
    
    try:
        # Fetch movie details from OMDB API
        async with aiohttp.ClientSession() as session:
            encoded_title = quote(movie_title)
            url = f"http://www.omdbapi.com/?apikey={OMDB_API_KEY}&t={encoded_title}"
            
            async with session.get(url) as response:
                response.raise_for_status()  # Raise exception for HTTP errors
                movie_data = await response.json()
                
                if movie_data.get("Response") == "True":
                    # Validate plot data
                    plot = movie_data.get("Plot", "")
                    if not plot or plot == "N/A":
                        logger.warning(f"No plot found for movie '{movie_title}'")
                        raise inngest.NonRetriableError(
                            message=f"No plot available for movie '{movie_title}'"
                        )
                    
                    logger.debug(f"Movie data retrieved for '{movie_title}'")
                    
                    # Process the movie data
                    summary = await summarize_plot_with_openai(plot)
                    
                    # Generate and send email
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
                    
                    # Process email delivery status
                    delivery_status = "unknown"
                    if email_result.get("success") and "delivery_status" in email_result:
                        delivery_status = email_result["delivery_status"].get("final_status", "unknown")
                        logger.info(f"Email delivery status for '{movie_title}': {delivery_status}")
                    
                    return {
                        "movie_title": movie_title,
                        "summary": summary,
                        "email_sent": email_result.get("success", False),
                        "email_delivery_status": delivery_status
                    }
                else:
                    error_message = movie_data.get("Error", "Unknown error")
                    logger.error(f"Movie '{movie_title}' not found in OMDB: {error_message}")
                    raise inngest.NonRetriableError(
                        message=f"Movie '{movie_title}' not found in OMDB: {error_message}"
                    )
    except inngest.NonRetriableError as e:
        logger.error(f"Non-retriable error: {str(e)}")
        raise
    except aiohttp.ClientError as e:
        logger.error(f"HTTP error when fetching movie data: {str(e)}")
        # This is retriable since it might be a temporary network issue
        raise Exception(f"Failed to fetch movie data: {str(e)}")