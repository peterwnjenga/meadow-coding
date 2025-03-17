import logging

import inngest

logger = logging.getLogger("uvicorn.inngest")
logger.setLevel(logging.DEBUG)

inngest_client = inngest.Inngest(app_id="meadow-app-interview", logger=logger)
