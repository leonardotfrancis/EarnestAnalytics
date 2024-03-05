import os

from fastapi import APIRouter, status, Depends
from fastapi.responses import FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional


from src.app.utils.auth_guard import AuthGuard
from src.app.general.service import MainService 
from src.app.general.schema import GroupByType


router = APIRouter(prefix='/general/v1',
                   tags=['General'], responses={404: {'description': 'Not Found'}})
security = HTTPBearer()

@router.post("/csv_data_ingestion", status_code=status.HTTP_200_OK)
def csv_data_ingestion(credentials: HTTPAuthorizationCredentials = Depends(security)):
    AuthGuard.decode_token(credentials.credentials)
        
    return MainService.csv_data_ingestion()

@router.get("/get_data", status_code=status.HTTP_200_OK)
def get_data(page_number: int = 1, credentials: HTTPAuthorizationCredentials = Depends(security)):
    AuthGuard.decode_token(credentials.credentials)
        
    return MainService.get_data(page_number, all_values=False)

@router.get("/get_all_data", status_code=status.HTTP_200_OK)
def get_all_data(credentials: HTTPAuthorizationCredentials = Depends(security)):
    AuthGuard.decode_token(credentials.credentials)
        
    return MainService.get_data(page_number=None, all_values=True)

@router.get("/get_category_by_age", status_code=status.HTTP_200_OK)
def get_category_by_age(group_by_type: GroupByType,credentials: HTTPAuthorizationCredentials = Depends(security)):
    AuthGuard.decode_token(credentials.credentials)
    
    return MainService.get_category_by_age(group_by_type.value)

@router.get("/get_age_amount", status_code=status.HTTP_200_OK)
def get_age_amount(credentials: HTTPAuthorizationCredentials = Depends(security)):
    AuthGuard.decode_token(credentials.credentials)
    
    return MainService.get_age_amount()