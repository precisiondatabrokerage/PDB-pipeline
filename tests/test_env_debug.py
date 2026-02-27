from dotenv import load_dotenv
import os

load_dotenv()

print("RAW DSN STRING BELOW:")
print(repr(os.getenv("POSTGRES_DSN")))
