import inngest
from urllib.parse import quote
from openai import OpenAI
import logging
import os
from dotenv import load_dotenv
import asyncio
import time
import uuid
import resend

from .client import inngest_client

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
# Set logging level
logger.setLevel(logging.INFO)
# Create handler if none exists
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
RESEND_API_KEY = os.getenv("RESEND_API_KEY")

# Initialize Resend client
resend.api_key = RESEND_API_KEY

async def summarize_plot_with_openai(plot_text: str) -> str:
    """
    Use OpenAI's GPT-4o-mini to summarize a movie plot.
    """
    client = OpenAI(api_key=OPENAI_API_KEY)
    
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": f"Please provide a concise summary of this movie plot: {plot_text}"
                }
            ]
        )
        
        return completion.choices[0].message.content
    except Exception as e:
        logger.error(f"Error calling OpenAI API: {str(e)}")
        return f"Failed to summarize plot: {str(e)}"

async def check_email_status(email_id: str) -> dict:
    """
    Check the delivery status of an email using Resend's API.
    """
    try:
        # Since resend SDK is synchronous, run it in a thread pool
        result = await asyncio.to_thread(resend.Emails.get, email_id=email_id)
        
        logger.info(f"Email status for {email_id}: {result.get('last_event', 'unknown')}")
        return {"success": True, "data": result}
                
    except Exception as e:
        error_message = f"Error checking email status: {str(e)}"
        logger.error(error_message)
        return {"success": False, "error": error_message}

async def poll_email_status(email_id: str, max_duration_seconds: int = 30) -> dict:
    """
    Poll the email status for up to max_duration_seconds.
    
    Args:
        email_id: The ID of the email to check
        max_duration_seconds: Maximum time to poll in seconds (default: 30)
        
    Returns:
        Dictionary with the final status information
    """
    start_time = time.time()
    poll_interval = 2  # Start with 2 second interval
    
    while time.time() - start_time < max_duration_seconds:
        status_result = await check_email_status(email_id)
        
        if not status_result["success"]:
            logger.warning(f"Failed to check email status: {status_result.get('error')}")
        else:
            from pprint import pformat
            logger.info(f"Email status result: {pformat(status_result)}")
            # Resend uses 'last_event' instead of 'status'
            status = status_result.get("data", {}).get("last_event")
            logger.info(f"Current email status: {status}")
            
            # If we have a definitive status, return it
            if status == "delivered":
                return {
                    "success": True,
                    "final_status": status,
                    "data": status_result.get("data", {})
                }
            elif status == "bounced":
                error_message = f"Email bounced: {status_result.get('data', {}).get('reason', 'Unknown reason')}"
                logger.error(error_message)
                raise inngest.NonRetriableError(message=error_message)
        
        # Wait before polling again, with exponential backoff (up to 5 seconds)
        await asyncio.sleep(min(poll_interval, 5))
        poll_interval *= 1.5  # Increase interval for next poll
    
    # If we've reached the time limit without a definitive status
    return {
        "success": True,
        "final_status": "unknown",
        "message": f"Email status still pending after {max_duration_seconds} seconds",
        "last_check": status_result.get("data", {})
    }

async def send_email_with_resend(recipient_email: str, subject: str, content: str, wait_for_status: bool = False) -> dict:
    """
    Send an email using Resend's API and optionally wait for delivery status.
    
    Args:
        recipient_email: Email address of the recipient
        subject: Email subject
        content: HTML content of the email
        wait_for_status: Whether to poll for delivery status (default: False)
        
    Returns:
        Dictionary with send result and status information if requested
    """
    # Generate a unique ID for tracking this email
    email_tracking_id = str(uuid.uuid4())
    
    payload = {
        "from": "Movie Summary <peter@atriumhq.us>",
        "to": recipient_email,
        "subject": subject,
        "html": content,
        "tags": [{"name": "email_id", "value": email_tracking_id}]
    }
    
    logger.info(f"Sending email to {recipient_email} with subject {subject}")
    try:
        # Use the Resend SDK to send the email
        result = await asyncio.to_thread(resend.Emails.send, payload)
        
        logger.info(f"Email sent successfully: {result.get('id')}")
        email_id = result.get('id')
        
        # If requested, poll for delivery status
        if wait_for_status and email_id:
            logger.info(f"Polling for email delivery status for up to 30 seconds...")
            status_result = await poll_email_status(email_id)
            return {
                "success": True, 
                "data": result, 
                "email_id": email_id,
                "delivery_status": status_result
            }
        
        return {"success": True, "data": result, "email_id": email_id}
                
    except Exception as e:
        error_message = f"Error sending email: {str(e)}"
        logger.error(error_message)
        
        # Determine if error is retriable
        if "rate limit" in str(e).lower() or "server error" in str(e).lower():
            return {"success": False, "error": error_message, "retriable": True}
        else:
            # Client errors are typically not retriable
            raise inngest.NonRetriableError(message=error_message)

def generate_movie_email_html(movie_data: dict, plot: str, summary: str, movie_title: str) -> str:
    """
    Generate HTML email content for movie summary.
    
    Args:
        movie_data: Dictionary containing movie information from OMDB API
        plot: Original movie plot
        summary: AI-generated summary of the plot
        movie_title: Title of the movie (fallback if not in movie_data)
        
    Returns:
        Formatted HTML string for email content
    """
    # Helper function to create Google search links for names
    def create_search_links(names_str):
        if not names_str or names_str == "N/A":
            return "N/A"
        
        names = [name.strip() for name in names_str.split(',')]
        linked_names = []
        
        for name in names:
            search_query = quote(f"{name} movie")
            linked_names.append(f'<a href="https://www.google.com/search?q={search_query}" target="_blank">{name}</a>')
        
        return ", ".join(linked_names)
    
    # Create linked versions of actors and directors
    linked_directors = create_search_links(movie_data.get('Director', 'N/A'))
    linked_actors = create_search_links(movie_data.get('Actors', 'N/A'))
    
    return f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; }}
            h1 {{ color: #2c3e50; }}
            .movie-card {{ display: flex; margin-bottom: 20px; }}
            .poster {{ margin-right: 20px; }}
            .poster img {{ max-width: 200px; border-radius: 4px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
            .details {{ flex: 1; }}
            .info-table {{ border-collapse: collapse; width: 100%; margin-top: 15px; }}
            .info-table td {{ padding: 8px; border-bottom: 1px solid #ddd; }}
            .info-table td:first-child {{ font-weight: bold; width: 30%; }}
            .ratings {{ margin-top: 15px; }}
            .summary-section {{ margin-top: 20px; background-color: #f9f9f9; padding: 15px; border-radius: 4px; }}
            a {{ color: #3498db; text-decoration: none; }}
            a:hover {{ text-decoration: underline; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Movie Summary: {movie_data.get('Title', movie_title)}</h1>
            
            <div class="movie-card">
                <div class="poster">
                    {f'<img src="{movie_data.get("Poster")}" alt="Movie poster">' if movie_data.get("Poster") and movie_data.get("Poster") != "N/A" else '<div style="width:200px;height:300px;background:#eee;display:flex;align-items:center;justify-content:center;border-radius:4px;">No poster available</div>'}
                </div>
                
                <div class="details">
                    <table class="info-table">
                        <tr><td>Year:</td><td>{movie_data.get('Year', 'N/A')}</td></tr>
                        <tr><td>Rated:</td><td>{movie_data.get('Rated', 'N/A')}</td></tr>
                        <tr><td>Runtime:</td><td>{movie_data.get('Runtime', 'N/A')}</td></tr>
                        <tr><td>Genre:</td><td>{movie_data.get('Genre', 'N/A')}</td></tr>
                        <tr><td>Director:</td><td>{linked_directors}</td></tr>
                        <tr><td>Actors:</td><td>{linked_actors}</td></tr>
                    </table>
                    
                    <div class="ratings">
                        <strong>Ratings:</strong><br>
                        {f"IMDb: {movie_data.get('imdbRating', 'N/A')}" if movie_data.get('imdbRating') and movie_data.get('imdbRating') != "N/A" else ""}
                        {f" | Metascore: {movie_data.get('Metascore', 'N/A')}" if movie_data.get('Metascore') and movie_data.get('Metascore') != "N/A" else ""}
                    </div>
                </div>
            </div>
            
            <div class="summary-section">
                <h2>Original Plot</h2>
                <p>{plot}</p>
                
                <h2>AI-Generated Summary</h2>
                <p>{summary}</p>
            </div>
        </div>
    </body>
    </html>
    """


