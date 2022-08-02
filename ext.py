import re
import yaml
import requests
import time
from urllib3.exceptions import InsecureRequestWarning

# Suppress the warning on Verify=False requests
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

tags = yaml.safe_load(open("tags.yaml"))
tags.append("aws-pds")

resources = yaml.safe_load(open("resources.yaml"))
services = yaml.safe_load(open("services.yaml"))

arn_regex = re.compile(r"^arn:(aws|aws-iso):.+:.*:.*:.+$")
host_regex = re.compile(r"^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,63})(\/.*)*\/?$")
controlled_access_regex = re.compile(
    r"^(https?:\/\/)?([\da-z\.\-\_]+)\.([a-z\.]{2,63})(\/.*)*\/?$"
)
explore_regex = re.compile(r"^\[.+\]\(https?:\/\/[\w\d.\-\/#\?\&\%=]+\)$")


def retry(howmany):
    def tryFunc(func):
        def f(*args, **kwargs):
            attempts = 0
            while attempts < howmany:
                try:
                    return func(*args, **kwargs)
                except:
                    attempts += 1
                    time.sleep(1.0 * 4.0 ** attempts)
                    if attempts >= howmany:
                        raise

        return f

    return tryFunc


# Check if provided tags are in tags.yaml
def ext_tags(value, rule_obj, path):
    if value not in tags:
        print("Invalid tag!", value)
        return False

    # If we're here, all tags were ok
    return True


# Check if provided resources are in resources.yaml
def ext_resources(value, rule_obj, path):
    if value not in resources:
        print("Invalid resource!", value)
        return False

    # If we're here, all resources were ok
    return True


# Check if provided services are in services.yaml
def ext_services(value, rule_obj, path):
    if value not in services:
        print("Invalid service!", value)
        return False

    # If we're here, all services were ok
    return True


# Check to make sure we have a valid arn
def ext_resources_arn(value, rule_obj, path):

    if not re.fullmatch(arn_regex, value):
        print(
            f"ARN '{value}' is not valid, it should like like arn:aws:s3:::yourbucket"
        )

        return False

    return True


# Check to make sure we have a valid host
def ext_resources_host(value, rule_obj, path):

    if not re.fullmatch(host_regex, value):
        print(f"Host '{value}' is not valid")
        return False

    return True


# Check to make sure we have a valid controlled access string
def ext_resources_controlled_access(value, rule_obj, path):

    if not re.fullmatch(controlled_access_regex, value):
        print(f"Controlled Access string '{value}' is not valid")
        return False

    return True


# Check to make sure we have a valid array of links
def ext_resources_explore(value, rule_obj, path):

    if not re.fullmatch(explore_regex, value):
        print(f"Explore string '{value}' is not a valid link")
        return False

    return True


@retry(5)
def get_bucket_region(url):
    # Get the headers for this bucket.
    # Verify=False because the wildcard matching doesn't work for buckets with '.'
    r = requests.head(url, verify=False)

    if r.status_code == requests.codes.not_found:
        print(r.headers)
        print(f"{r.status_code} {r.reason} {r.url}")
        raise Exception(f"Bucket {url} doesn't exist or there was a momentary glitch")

    if "x-amz-bucket-region" not in r.headers:
        print(r.headers)
        print(f"{r.status_code} {r.reason} {r.url}")
        raise Exception("Bucket region missing from request header?")

    return r.headers["x-amz-bucket-region"]


def ext_valid_bucket_regions(value, rule_obj, path):

    # Validate required fields in resources
    if not isinstance(value, dict):
        print("Did not receives a resources dictionary...")
        return False

    if "Type" not in value:
        print("Type is a required resources field")
        return False

    if "Description" not in value:
        print("Description is a required resources field")
        return False

    if "Region" not in value:
        print("Region is a required resources field")
        return False

    if "Explore" in value and not isinstance(value["Explore"], list):
        print("Explore must be an array of links")
        return False

    # Make sure this is a dict, and a bucket, then validate the region
    if value["Type"] == "S3 Bucket":
        bucket = value["ARN"]
        parts = bucket.split(":::")
        if parts[0] != "arn:aws:s3":
            # This is probably not on public aws so we can't check
            return True
        bucket = parts[1]
        parts = bucket.split("/")
        bucket = parts[0]
        url = f"https://{bucket}.s3.amazonaws.com"

        region = get_bucket_region(url)
        if value["Region"].lower() != region.lower():
            print(
                f'The region for bucket {bucket} is listed as {value["Region"]} but is actually {region}'
            )

            return False

    return True
