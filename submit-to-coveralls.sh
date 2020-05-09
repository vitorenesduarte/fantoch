#!/bin/sh

### BASICALLY A COPY OF: https://github.com/newsboat/newsboat/blob/master/submit-to-coveralls.sh

if [ "${TRAVIS_EVENT_TYPE}" = "pull_request" ]
then
    branch="${TRAVIS_PULL_REQUEST_BRANCH}"
else
    branch="${TRAVIS_BRANCH}"
fi

if [ "${TRAVIS_PULL_REQUEST}" = "true" ]
then
    service_pull_request="--service-pull-request ${TRAVIS_PULL_REQUEST}"
fi

if [ -n "${COVERALLS_REPO_TOKEN}" ]
then
    token="--token ${COVERALLS_REPO_TOKEN}"
fi

grcov . \
    ${token} \
    ${service_pull_request} \
    --service-number "${TRAVIS_BUILD_ID}" \
    --service-name travis-ci \
    --service-job-number "${TRAVIS_JOB_ID}" \
    --commit-sha "${TRAVIS_COMMIT}" \
    --vcs-branch "${branch}" \
    --ignore-not-existing \
    --ignore='/*' \
    -t coveralls \
    -o coveralls.json

curl \
    --form "json_file=@coveralls.json" \
    --include \
    https://coveralls.io/api/v1/jobs

