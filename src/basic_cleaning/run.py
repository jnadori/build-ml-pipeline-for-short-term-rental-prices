#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    logger.info("Downloading database to clean")
    df = pd.read_csv(artifact_local_path)
    logger.info("Cleaning database")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    df['last_review'] = pd.to_datetime(df['last_review'])
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    logger.info("Exporting database")
    df.to_csv("clean_sample.csv", index=False)
    logger.info(f"Uploading clean database to Weights and Biases as{args.output_artifact}")
    artifact = wandb.Artifact(
    args.output_artifact,
       type=args.output_type,
       description=args.output_description, )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


    


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="input artifact to clean", )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Output artifact path",
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the output artifact",
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the output",
    )

    parser.add_argument(
        "--min_price", 
        type=int,
        help="The minimum price point",
    )

    parser.add_argument(
        "--max_price", 
        type=int,
        help="The maximum price point",
    )


    args = parser.parse_args()

    go(args)
