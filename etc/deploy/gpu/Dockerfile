FROM ubuntu:18.04

RUN apt-get update && apt-get install -y sudo && apt-get install -y wget && rm -rf /var/lib/apt/lists/* 
ENV NVIDIA_RUNNER NVIDIA-Linux-x86_64-375.51.run
RUN wget http://us.download.nvidia.com/XFree86/Linux-x86_64/375.51/${NVIDIA_RUNNER} && \
	chmod +x ${NVIDIA_RUNNER}

ADD install-nvidia-driver.sh .
ADD install.sh .

CMD sudo ./install.sh ${NVIDIA_RUNNER}
