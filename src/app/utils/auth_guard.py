import os
from fastapi import HTTPException, status

class AuthGuard:
    
    @staticmethod
    def decode_token(token: str):
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"Authorization": "Bearer"},
        )
        
        if token is None:
            raise credentials_exception
    
        remove_bearer = token.replace("Bearer ", "")

        try:
            oauth_token = os.environ["SECRET"]
            if oauth_token == remove_bearer:
                return True
            else:
                raise credentials_exception
        except Exception as e:
            raise credentials_exception