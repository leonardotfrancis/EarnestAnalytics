import os, requests

from fastapi import FastAPI, Request, HTTPException as FPHTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError


from src.app.general import controller as main_controller


APP_VERSION = os.environ["VERSION"]


app = FastAPI(
    title="Main API",
    version=APP_VERSION,
)

# health check
@app.get("/", tags=['Home'])
def root():
    return {
      "status": 'ok',
      "message": 'Server is up and running!',
      "version": APP_VERSION
    }


app.include_router(main_controller.router)