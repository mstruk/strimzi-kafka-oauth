Building and deploying Strimzi Kafka OAuth
==========================================

You only need Java 8, and Maven to build this project. 

However, you may want to rebuild Strimzi Kafka Operator project components and images to try your changes on Kubernetes.
Setting up a build environment for that is not trivial, so we have prepared a docker image with all the necessary build tools.

We call it Strimzi Dev CLI Image.

In order to build Strimzi Kafka Operator images you need a running Docker daemon.
If you also want to try them out, deploying the Kafka Cluster Operator, and running a test cluster on Kubernetes, you need access to Kubernetes API server.

There are several locally running options for Kubernetes: [Kubernetes Kind](https://github.com/kubernetes-sigs/kind), [Minikube](https://github.com/kubernetes/minikube), [Minishift](https://github.com/minishift/minishift), possibly others ...

Here are instructions for setting up several different host environments with Docker and Kubernetes Kind so that you can then use a strimzi-dev-cli shell session to build and deploy Strimzi Kafka Operator using locally built images containing the latest, locally built, Strimzi Kafka OAuth libraries.


Preparing the host environment
------------------------------

### Ubuntu 18.04 LTS

#### Installing and configuring Docker daemon

Run the following commands to install the Docker package from the Ubuntu repository:

    sudo apt-get update
    sudo apt-get remove docker docker-engine docker.io
    sudo apt install docker.io


Run the following to configure the Docker daemon to trust a local Docker Registry listening on port 5000:

```
export REGISTRY_IP=$(ifconfig docker0 | grep 'inet ' | awk '{print $2}') && echo $REGISTRY_IP

sudo cat << EOF > /etc/docker/daemon.json
{
  "debug": true,
  "experimental": false,
  "insecure-registries": [
    "${REGISTRY_IP}:5000"
  ]
}
EOF
```

Start or restart the daemon:

    sudo systemctl restart docker

Enable the daemon so it's automatically started when system boots up:

    sudo systemctl enable docker

Fix Permission denied issue when running `docker` client:

    sudo groupadd docker
    sudo usermod -aG docker $USER

Test that everything works:

    docker ps


#### Installing `kubectl`

Ubuntu supports `snaps` which is the easiest way to install `kubectl`:

    snap install kubectl --classic
    kubectl version
    

#### Installing Kubernetes Kind

[Kubernetes Kind](https://github.com/kubernetes-sigs/kind) is a Kubernetes implementation that runs on Docker. That makes it simple to install, and convenient to use.

You can install by running the following:

    curl -Lo ./kind "https://github.com/kubernetes-sigs/kind/releases/download/v0.7.0/kind-$(uname)-amd64"
    chmod +x ./kind
    sudo mv ./kind /usr/local/bin/kind


### Docker Desktop for Mac

On MacOS the most convenient option for Docker is to use [Docker Desktop](https://www.docker.com/products/docker-desktop).


#### Configuring Docker daemon

Using Docker Desktop for Mac, first make sure to assign enough memory.
Double-click on Docker icon in tool bar and select 'Preferences', then 'Resources'.

The actual requirements for resources dependends on what exactly you'll be doing but the following configuration should be enough if you want to do small cluster deployments with Strimzi.

Under Memory select 5 GB. For swap size select at least 2 GB. For CPUs select at least 2 (that's quite important).

Click `Apply & Restart` button.

We'll setup Kind to use a local Docker Registry deployed as a Docker container.
In order to allow non-tls connectivity between Docker daemon and Docker Registry we need configure Docker daemon. 

Open a Terminal and type:

    export REGISTRY_IP=$(ifconfig en0 | grep 'inet ' | awk '{print $2}') \
      && echo $REGISTRY_IP

Move on to 'Preferences' / 'Docker Engine' tab.

There is a Docker Engine configuration file. It's current content typically looks something like:

```
{
  "debug": true,
  "experimental": false
}
``` 

We'll add another array attribute called `insecure-registries` that will contain our $REGISTRY_IP and port.
For example:
```
{
  "debug": true,
  "experimental": false,
  "insecure-registries": [
    "192.168.1.10:5000"      <<< Use your REGISTRY_IP
  ]
}
``` 

Click `Apply & Restart` button again.


#### Installing `kubectl`

Simplest way to install `kubectl` on MacOS is to use Homebrew:

    brew install kubectl
    kubectl version


#### Installing Kubernetes Kind

[Kubernetes Kind](https://github.com/kubernetes-sigs/kind) is a Kubernetes implementation that runs on Docker. That makes it simple to install, and convenient to use.

On MacOS the most convenient way to install is to use Homebrew:

    brew install kind


Starting up the environment
---------------------------

The rest of what we do is platform independent. All we need are working `docker`, `kind`, and `kubectl`.

Everytime you start a new Terminal shell, make sure to set the following ENV variables:

```
export REGISTRY_IP=<YOUR REGISTRY IP FROM PREVIOUS CHAPTER>
export KIND_CLUSTER_NAME=kind
export REGISTRY_NAME=docker-registry
export REGISTRY_PORT=5000
```

### Deploying and validating Docker Registry

Execute the following:

    docker run -d --restart=always -p "$REGISTRY_PORT:$REGISTRY_PORT" --name "$REGISTRY_NAME" registry:2

The registry should be up an running within a few second.

Let's make sure that we can push images to the registry using $REGISTRY_IP:

```
docker pull gcr.io/google-samples/hello-app:1.0
docker tag gcr.io/google-samples/hello-app:1.0 $REGISTRY_IP:$REGISTRY_PORT/hello-app:1.0
docker push $REGISTRY_IP:$REGISTRY_PORT/hello-app:1.0
```

### Creating and validating Kind Kubernetes cluster

When starting Kind we need to pass some extra configuration to allow Kubernetes instance to connect to insecure Docker Registry from a previous step.

```
cat << EOF | kind create cluster --name "${KIND_CLUSTER_NAME}" --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
containerdConfigPatches: 
- |-
  [plugins."io.containerd.grpc.v1.cri".registry.mirrors."$REGISTRY_IP:$REGISTRY_PORT"]
    endpoint = ["http://$REGISTRY_IP:$REGISTRY_PORT"]
EOF
```

Note, how we use `http` in `endpoint` value, which does the trick.

Let's make sure we can deploy Kubernetes Pod using image from a local Docker Registry:

    docker tag gcr.io/google-samples/hello-app:1.0 $REGISTRY_IP:$REGISTRY_PORT/hello-app:1.0
    kubectl create deployment hello-server --image=$REGISTRY_IP:$REGISTRY_PORT/hello-app:1.0
    kubectl get pod

By repeating the last command we should after a few seconds see it turn to `Running` status for `hello-server-*`.
If there is an error status, see [Troubleshooting](#troubleshooting) chapter.

You can now remove the deployment:

    kubectl delete deployment hello-server

One important thing before deploying Strimzi Kafka Operator on Kind is to give the system account 'cluster-admin' permissions:

    kubectl create clusterrolebinding strimzi-cluster-operator-cluster-admin --clusterrole=cluster-admin --serviceaccount=default:strimzi-cluster-operator


### Starting and validating Strimzi Dev CLI

In a new Terminal shell execute the following:

```
# Get internal configuration for access from within container:
kind get kubeconfig --internal > ~/.kube/internal-kubeconfig

# Make sure to use latest version of image
docker pull quay.io/mstruk/strimzi-dev-cli

# set DEV_DIR to directory where you have your cloned git repositories
# You'll be able to access this directory from within Strimzi Dev CLI container
export DEV_DIR=$HOME/devel

# Now run the container
docker run -ti --name strimzi-dev-cli -v /var/run/docker.sock:/var/run/docker.sock -v $HOME/.kube:/root/.kube -v $DEV_DIR:/root/devel -v $HOME/.m2:/root/.m2:cached quay.io/mstruk/strimzi-dev-cli /bin/sh
```

Note: If you exit the container or it gets shut down, as long as it's not manually deleted you can reattach and continue interactive session:

    docker start strimzi-dev-cli
    docker attach strimzi-dev-cli

Having starterd the interactive session you are now in the development environment where you have all the necessary tools including `docker`, `kind`, `kubectl`, `git`, 'mvn' and all the rest you need to build Strimzi Kafka Operator components.

Let's make sure that `docker`, and `kubectl` work:

```
export KUBECONFIG=~/.kube/internal-kubeconfig
kubectl get ns
docker ps
```

Also, let's make sure that we can push to local Docker Registry from Strimzi Dev CLI:

```
# Set REGISTRY_IP to the same value it has in the other Terminal session
export REGISTRY_IP=<enter-the-ip-of-your-en0>

export REGISTRY_PORT=5000

# test docker push to local repository
docker tag gcr.io/google-samples/hello-app:1.0 $REGISTRY_IP:$REGISTRY_PORT/hello-app:1.0
docker push $REGISTRY_IP:$REGISTRY_PORT/hello-app:1.0

```

Building Strimzi Kafka OAuth
----------------------------

If you have not yet cloned the source repository it's time to do it now.
It's best to fork the project and clone your fork, but you can also just clone the upstream repository.

```
cd /root/devel
git clone https://github.com/strimzi/strimzi-kafka-oauth.git
cd strimzi-kafka-oauth

# Build it
mvn clean spotbugs:check install

# Make sure it's PR ready
# Sorry, unfortunately the testsuite doesn't seem to be working inside strimzi-dev-cli
# .travis/build.sh
```

Deploying development builds with Strimzi Kafka Operator
--------------------------------------------------------

After successfully building Strimzi Kafka OAuth artifacts you can include them into Strimzi Kafka images by following either of these two approaches:

* Rebuild Strimzi Kafka Operator project images from source, referring to SNAPSHOT (non-released) Strimzi Kafka OAuth artifacts
* Build custom Strimzi component images based on existing ones


### Building Strimzi Kafka images with SNAPSHOT version of Strimzi Kafka OAuth

Let's clone the upstream repository:

```
cd /root/devel
git clone https://github.com/strimzi/strimzi-kafka-operator.git
cd strimzi-kafka-operator
```

We have to update the oauth library dependency version:

    sed -Ei 's#<strimzi-oauth.version>[0-9a-zA-Z.-]+</strimzi-oauth.version>#<strimzi-oauth.version>1.0.0-SNAPSHOT</strimzi-oauth.version>#g' \
      pom.xml \
      docker-images/kafka/kafka-thirdparty-libs/2.3.x/pom.xml \
      docker-images/kafka/kafka-thirdparty-libs/2.4.x/pom.xml

This makes sure the latest strimzi-kafka-oauth library that we built previously is included into Kafka images that we'll build next.
We can check the change:

    git diff
    
We're ready to build a SNAPSHOT version of strimzi-kafka-operator.

    MVN_ARGS=-DskipTests make clean docker_build

Build Strimzi Docker images containing Kafka with Strimzi OAuth support:

    export DOCKER_REG=$REGISTRY_IP:$REGISTRY_PORT
    DOCKER_REGISTRY=$DOCKER_REG DOCKER_ORG=strimzi make docker_push

If everything went right we should have the built images in our local Docker Registry.

    docker images | grep $REGISTRY_IP:$REGISTRY_PORT
    
Note, that if you make changes to Strimzi Kafka OAuth and have to rebuild with the new images you can just do the following
instead of doing the whole `docker_build` again:

    make -C docker-images clean build
    DOCKER_REGISTRY=$DOCKER_REG DOCKER_ORG=strimzi make docker_push
    
Let's make sure the SNAPSHOT Strimzi OAuth libraries are included.

    docker run --rm -ti $DOCKER_REG/strimzi/kafka:latest-kafka-2.4.0 /bin/sh -c 'ls -la /opt/kafka/libs/kafka-oauth*'

This executes a `ls` command inside a new Kafka container, which it removes afterwards.
The deployed version should be 1.0.0-SNAPSHOT.


### Building custom Strimzi Kafka 'override' image based on existing one

Instead of rebuilding the whole Strimzi Kafka Operator project to produce initial Kafka images, we can simply adjust an existing image so that our newly built libraries are used instead of the ones already present in the image.

We can simply use a `docker build` command with a custom Dockerfile. This is very convenient for quick iterative development.
Any step you can shorten can cumulatively save you a lot of time, and building the whole Strimzi Kafka Operator project, for example, takes quite some time.

You can follow the instructions in a previous chapter to build the initial local version of Strimzi Kafka Operator and Strimzi Kafka images. The 'override' images can then be based on ones you built from source.

Alternatively, you can avoid cloning and building the Strimzi Kafka Operator project altogether by basing the 'override' image on existing publicly available Strimzi Kafka image. 

In `examples/docker/strimzi-kafka-image` there is a build project that takes the latest strimzi/kafka image, and adds another layer to it where it copies latest SNAPSHOT kafka-oauth libraries into the image, and prepends the directory containing them to the CLASSPATH thus making sure they override the previously packaged versions, and their dependencies. 

See [README.md](examples/docker/strimzi-kafka-image/README.md) for instructions on how to build and use the 'override' Kafka image.


### Configuring Kubernetes permissions

Make sure to give strimzi-cluster-operator service account the necessary permissions. It depends on Kubernetes implementation you're using how to achieve that.

Using `Kind` you can do:

    kubectl create clusterrolebinding strimzi-cluster-operator-cluster-admin --clusterrole=cluster-admin --serviceaccount=default:strimzi-cluster-operator

Using `Minishift` you can run:

    oc login -u system:admin
    oc adm policy add-cluster-role-to-user cluster-admin developer
    oc login -u developer

 
### Deploying Kafka operator and Kafka cluster

You can deploy Strimzi Cluster Operator the usual way:

    kubectl apply -f install/cluster-operator

Note: If you're running Kubernetes within a VM you need at least 2 CPUs.

Using `kubectl get pod` you should see the status of strimzi-cluster-operator pod become `Running` after a few seconds.

If not, you can see what's going on by running:

    kubectl describe pod $(kubectl get pod | grep strimzi-cluster-operator | awk '{print $1}')


You can then deploy an example Kafka cluster:

    kubectl apply -f examples/kafka/kafka-ephemeral-single.yaml

Make sure the kafka-oauth-* libraries are present:

    kubectl exec my-cluster-kafka-0 /bin/sh -c 'ls libs/oauth'
    
You can follow the Kafka broker log:

    kubectl logs my-cluster-kafka-0 -c kafka


### Deploying a Kafka cluster configured with OAuth 2 authentication

Rather than using a basic Kafka cluster without any authentication you'll need one with OAuth 2 authentication and / or authorization in order to test Strimzi Kafka OAuth.

For examples of deploying such a cluster see [/examples/kubernetes/README.md](examples/kubernetes/README.md)


### Exploring the Kafka container

You can explore the Kafka container more by starting it in interactive mode:

    docker run --rm -ti $DOCKER_REG/strimzi/kafka:latest-kafka-2.4.0 /bin/sh
 
Here you've just started another interactive container from within the existing interactive container session.
Pretty neat!

Let's set a custom prompt so we don't get confused which session it is:

    export PS1="strimzi-kafka\$ "

Let's check oauth library versions:
    
    ls -la /opt/kafka/libs/kafka-oauth*

Once you're done exploring, leave the container by issuing:

    exit

The interactive container will automatically get deleted because we used `--rm` option.


Troubleshooting
---------------

### Error message: Server gave HTTP response to HTTPS client

When pushing to Docker Registry, for example, when running:

    DOCKER_REGISTRY=$DOCKER_REG DOCKER_ORG=strimzi make docker_push

You get an error like:
 
    Get https://192.168.1.86:5000/v2/: http: server gave HTTP response to HTTPS client

The reason is that Docker Daemon hasn't been configured to treat Docker Registry with the specified IP as insecure repository.

If you are switching between WiFis, your local network IP keeps changing. If using Kind, the mirror configuration used when starting Kind to allow access to insecure registry over http may be out of sync with your current local network IP.
Removing current Kind cluster and creating a new one should solve the issue. 

You may also have to update your Docker Desktop or Docker Daemon configuration to add the new IP to `insecure-registries`.

See [Configuring Docker daemon](#configuring-docker-daemon) for how to configure `insecure-registries`.


### Error message: node(s) already exist for a cluster with the name "kind"

When creating a new Kubernetes cluster with Kind you can get this error.
It means that the cluster exists already, but you may not see when you do:

    docker ps

Try the following:

    docker ps -a | grep kind-control-plane

The `kind-control-plane` container may simply be stopped, and you can restart it with:

    docker start kind-control-plane

If you're in an environment where your local network ip changes (moving around with a laptop for examnple) it's safest to just remove the cluster and then create it from scratch:

    kind delete cluster