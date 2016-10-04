import argparse
import calendar
from datetime import datetime, timedelta
import logging
import json

import requests


PROJECT_SITE = "https://review.openstack.org/changes/"
QUERY = "q=project:openstack/nova"
ATTRS = ("&o=CURRENT_REVISION&o=ALL_COMMITS&o=ALL_FILES&o=LABELS"
         "&o=DETAILED_LABELS&o=DETAILED_ACCOUNTS")

TODO_LIST = 'to_be_transformed'

BURNDOWN_DATA_CSV = 'data.csv'
TODO_LIST_JSON_FILE = 'data.json'

LOG = logging.getLogger()


def parse_args():
    parser = argparse.ArgumentParser(
        description='Get the status of the transformation')
    parser.add_argument('-d', '--debug', action='store_true',
                        dest="debug",
                        default=False)
    return parser.parse_args()


def load_to_do_list():
    with open(TODO_LIST) as todo:
        return [line.strip() for line in todo]


def todo_to_notification_sample(event_types):
    samples = []
    for event_type in event_types:
        samples.append('-'.join(event_type.replace('.', '-').split('-')[0:2]) +
                       '-')  # add a last hyphen to avoid false matches
    return samples


def fetch_gerrit_data(sample_file_names):
    file_query = ""
    for sample in sample_file_names:
        file_query += "file:^.*%s.*+OR+" % sample
    file_query = file_query[0:-4]  # remove the last OR

    query = QUERY + "+(%s)" % file_query
    URL = "%s?%s%s" % (PROJECT_SITE, query, ATTRS)

    resp = requests.get(URL)
    # slice out the "safety characters"
    content = resp.content[5:]

    LOG.debug("Response from Gerrit")
    LOG.debug(content)

    return json.loads(content)


def match_reviews_for_samples(reviews, samples):
    result = {}
    for sample in samples:
        result[sample] = get_review_adding_sample(sample, reviews)

    return result


def get_review_adding_sample(sample, reviews):
    for review in reviews:
        revs = review['revisions']
        if is_add_sample_file(sample, revs[revs.keys()[0]]['files']):
            return review
    return None


def is_add_sample_file(sample, change_list):
    for path, change in change_list.items():
        if ('doc/notification_samples' in path and sample in path and
                change['status'] == 'A'):
            return True
    return False


def write_burndown_to_csv(reviews):
    with open(BURNDOWN_DATA_CSV, 'w') as csv_file:
        csv_file.write('date, to_be_transformed\n')
        hourly_data = get_hourly_burndown_data(reviews, datetime(2016, 6, 1))
        for date, todo in hourly_data:
            timestamp = calendar.timegm(date.timetuple())
            csv_file.write('%d, %d\n' % (timestamp, todo))


def get_hourly_burndown_data(reviews, start):
    hourly_data = []
    time = start
    while time < datetime.now():
        hourly_data.append((time, get_burndown_data(reviews, time)))
        time += timedelta(hours=1)
    return hourly_data


def get_burndown_data(reviews, until):
    to_be_transformed = 0
    for sample, review in sorted(reviews.items()):
        if (not review or
            review['status'] != 'MERGED' or
            (review['status'] == 'MERGED' and
             datetime.strptime(review['submitted'],
                               '%Y-%m-%d %H:%M:%S.%f000') > until)):
            to_be_transformed += 1
    return to_be_transformed


def write_todo_list_to_json(reviews):
    result = []
    for sample, review in sorted(reviews.items()):
        if review:
            result.append({
                'event_type': sample[:-1],
                'status': review['status'],
                'review': review['_number'],
                'owner': review['owner']['username']
            })
        else:
            result.append({
                'event_type': sample[:-1],
                'status': 'TODO',
                'review': '',
                'owner': ''
            })
    with open(TODO_LIST_JSON_FILE, 'w') as jf:
        json.dump(result, jf)


def main():
    args = parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    samples = todo_to_notification_sample(load_to_do_list())
    reviews = fetch_gerrit_data(samples)
    reviews = match_reviews_for_samples(reviews, samples)
    write_todo_list_to_json(reviews)
    write_burndown_to_csv(reviews)


if __name__ == "__main__":
    main()