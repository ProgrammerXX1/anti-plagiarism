from fastapi import HTTPException

def bad_request(msg: str) -> HTTPException:
    return HTTPException(400, msg)

def not_found(msg: str) -> HTTPException:
    return HTTPException(404, msg)

def server_error(msg: str) -> HTTPException:
    return HTTPException(500, msg)
