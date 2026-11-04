# Official Hadoop image + python3 (needed by Hadoop Streaming mappers/reducers).
FROM apache/hadoop:3.3.6
USER root
# The base image is CentOS-7 derived; its mirrors are EOL, so point yum at the vault first.
RUN (sed -i 's/^mirrorlist/#mirrorlist/; s|^#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|' /etc/yum.repos.d/*.repo 2>/dev/null || true) \
 && (yum install -y python3 || dnf install -y python3 || microdnf install -y python3 || apt-get update && apt-get install -y python3) \
 && (python3 --version || echo "WARNING: python3 not installed - use scripts/run_mr_local.sh instead")
USER hadoop
