FROM python:3

WORKDIR /datacommons

COPY . /datacommons

# Install Bazel build tool
RUN echo "deb [arch=amd64] http://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list
RUN curl https://bazel.build/bazel-release.pub.gpg | apt-key add -

# Install required packages
RUN apt-get -q update && \
  apt-get -q -y install --no-install-recommends \
  bazel \
  time

# Install python
RUN python setup.py -q install

# Run the tests
RUN ./build.sh
