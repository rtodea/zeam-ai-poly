from zeam.scheduler.celery_app import app
import sys

def main():
    sys.argv.append("beat")
    app.start()

if __name__ == "__main__":
    main()
