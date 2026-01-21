from zeam.celery_core.core import app
import sys

def main():
    sys.argv.append("worker")
    app.start()

if __name__ == "__main__":
    main()
