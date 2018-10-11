# Data Commons presubmit tests and continuous integration

![Kokoro Ubuntu](https://storage.googleapis.com/datacommons-ci/status.svg)

Data Commons is tested continuously with
[Kokoro](https://www.cloudbees.com/sites/default/files/2016-jenkins-world-jenkins_inside_google.pdf)
an internal deployment of Jenkins at Google.

Continuous builds and presubmit tests are run on Ubuntu.

Presubmit tests are triggered for the pull request if one of the following
conditions is met:

*  The pull request is created by a Googler.
*  The pull request is attached with a kokoro:run label.

Continuous builds are triggered for every new commit.
