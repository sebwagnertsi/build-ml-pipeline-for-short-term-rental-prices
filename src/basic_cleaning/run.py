#!/usr/bin/env python
"""
Performs basic cleaning on the data and store the resulting artifacts in wandb
"""
import argparse
import logging
import wandb
import os
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    input_data = pd.read_csv(artifact_local_path, low_memory=False)
    # print(input_data.head())

    # Compute basic cleaning steps:

    min_price = args.min_price
    max_price = args.max_price
    idx = input_data['price'].between(min_price, max_price)
    input_data = input_data[idx].copy()
    logger.info("Basic cleaning: Price Filtering completed.")

    # Filter out locations that are not in new york
    idx = input_data['longitude'].between(-74.25, -
                                          73.50) & input_data['latitude'].between(40.5, 41.2)
    input_data = input_data[idx].copy()

    # Convert last_review to datetime
    input_data['last_review'] = pd.to_datetime(input_data['last_review'])
    logger.info("Basic cleaning: Date conversion completed.")

    # Log the artifact
    outputfile = os.path.abspath('temp_output.csv')
    logger.info(f"Temp output file stored in {outputfile}")
    input_data.to_csv(outputfile, index=False)
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(outputfile)

    run.log_artifact(artifact)

    # Wait for upload completion
    artifact.wait()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Clean the data")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help='the input artifact which will be processed.',
        required=True
    )
    parser.add_argument(
        "--output_artifact",
        type=str,
        help='the resulting output artifact.',
        required=True
    )
    parser.add_argument(
        "--output_type",
        type=str,
        help='Type of the output.',
        default='parquet',
        required=False
    )
    parser.add_argument(
        "--output_description",
        type=str,
        help='The description of our output artifact',
        default='A cleaned version of the input file.',
        required=False
    )
    parser.add_argument(
        "--min_price",
        type=float,
        help='The minimum legitimate price.',
        required=True
    )
    parser.add_argument(
        "--max_price",
        type=float,
        help='The maximum legitimate price.',
        required=True
    )

    args = parser.parse_args()

    print("BASIC CLEANING:", args)

    go(args)
