import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import graph

app = FastAPI()
app.include_router(graph.router)

origins = [
    "http://localhost",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/')
def index():
    return {'message': 'Hello world!!!'}

if __name__ == "__main__":
    uvicorn.run("main:app",
                 host="localhost",
                 port=8432, 
                 reload=True)