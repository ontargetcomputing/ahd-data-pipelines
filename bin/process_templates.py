import os
from jinja2 import Environment, FileSystemLoader
import argparse
import yaml
import os
import shutil
import pathlib


def render_template(template_path, data):
    # Create Jinja2 environment
    env = Environment(loader=FileSystemLoader(os.path.dirname(template_path)))
    # Load the template
    template = env.get_template(os.path.basename(template_path))
    # Render the template with data
    rendered = template.render(data)
    return rendered


def render_databricks_config(template_path, data, output_dir):
    rendered = render_template(template_path, data)
    # Save the output to a new file
    output_filename = os.path.basename(template_path)
    output_path = os.path.join(output_dir, output_filename)
    with open(output_path, "w+") as output_file:
        output_file.write(rendered)


def process_directory(template_path, data, output_dir):
    print(f"Processing directory {template_path}")
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Iterate over files in the directory
    for filename in os.listdir(template_path):
        # Skip data files
        if filename.startswith("data."):
            continue

        file_path = os.path.join(template_path, filename)
        print(f"Found file path {file_path}")
        if os.path.isdir(file_path):

            if filename != "workflows":
                process_directory(
                    file_path, data, os.path.join(output_dir, file_path.split("/")[-1])
                )
        else:
            print(f"Processing file {file_path}")
            if data["workflows"].get("_".join(template_path.split("/")[2:])) is not None:
                # Render the template with data
                rendered = render_template(file_path, data)

                # Save the output to a new file
                output_filename = os.path.basename(file_path)
                output_path = os.path.join(output_dir, output_filename)
                with open(output_path, "w+") as output_file:
                    output_file.write(rendered)
            else:
                print("Skipping")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process files in a directory using a template."
    )
    parser.add_argument("--env", default="dev", help="Path to the data file")
    parser.add_argument(
        "--template_path",
        default="conf.j2/parameters",
        help="Path to the directory containing the template files",
    )
    parser.add_argument(
        "--build_path",
        default="conf/parameters",
        help="Path to the directory to output to",
    )
    parser.add_argument(
        "--databricks_template_path",
        default="conf.j2/databricks.yml",
        help="Path to the databricks.yml template",
    )
    parser.add_argument(
        "--databricks_build_path",
        default=".",
        help="Path to output the databricks config",
    )
    args = parser.parse_args()

    data_file_path = f"conf.j2/data.{args.env}.yml"
    # Load data
    with open(data_file_path, "r") as data_file:
        data = yaml.safe_load(data_file)

    # Process the directory
    render_databricks_config(
        args.databricks_template_path, data, args.databricks_build_path
    )
    process_directory(args.template_path, data, args.build_path)
