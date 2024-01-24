import yaml
import sys
import pathlib
from ahd_data_pipelines.dependencies import example
from argparse import ArgumentParser


def main():
  print("1" )



  p = ArgumentParser()
  p.add_argument("--conf-file", required=False, type=str)
  namespace = p.parse_known_args(sys.argv[1:])[0]
  conf_file = namespace.conf_file
  print(conf_file)
  print("3")
  config = yaml.safe_load(pathlib.Path(conf_file).read_text())
  print(config)
  example.hello(config['name'])



if __name__ == '__main__':
  main()   
  