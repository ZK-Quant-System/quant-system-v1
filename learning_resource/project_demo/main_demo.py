import json
import glog
import click
from config import demo_config
from utils import change_tone



class Speaker:
    def __init__(self, config):
        self.voice = demo_config.voice
        self.other_words = config["other_words"]

    def speak(self):
        if self.voice == 0:
            glog.info(f"~hi~")
        elif self.voice == 1:
            glog.info(f"{change_tone.tune_up('hi')}")
        elif self.voice == 2:
            print(f"Hi {self.other_words}")
        elif self.voice == 3:
            print(f"HI")
        else:
            pass


@click.command()
@click.option("--config_file", help="the config file for speaker", default="./config/config.json")
def main(config_file):
    with open(config_file) as f:
        speaker_config = json.load(f)
    speaker = Speaker(speaker_config)
    speaker.speak()


if __name__ == "__main__":
    main()
