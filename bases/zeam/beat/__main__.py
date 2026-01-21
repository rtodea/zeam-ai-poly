from zeam.celery_core.core import app
from zeam.beat.schedule import get_beat_schedule
import sys

def main():
    # Configure the beat schedule before starting
    app.conf.beat_schedule = get_beat_schedule()
    
    sys.argv.append("beat")
    app.start()

if __name__ == "__main__":
    main()
