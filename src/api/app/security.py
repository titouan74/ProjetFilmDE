from fastapi import HTTPException, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets

security = HTTPBasic()

USERS = {
    "titouan":{"password":"datascientest1","role":"admin"},
    "cynthia":{"password":"datascientest2","role":"admin"},
    "user":{"password":"datascientest3","role":"user"}
}

def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    user = USERS.get(credentials.username)
    if not user or not secrets.compare_digest(user["password"], credentials.password):
        raise HTTPException(status_code=401, detail="Identifiants incorrects")
    return {"username": credentials.username,"role":user["role"]}

def verify_admin(user: dict = Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Accès réservé aux admins")
    return user
