ssh -F ~/DevProjects/MicroK8s/terraform/production/k8s_ssh.conf -f -N -L 10001:localhost:30001 k8s-control-1
ssh -F ~/DevProjects/MicroK8s/terraform/development/k8s_ssh.conf -f -N -L 20001:localhost:30001 k8s-control-1
ssh -F ~/DevProjects/MicroK8s/terraform/production/k8s_ssh.conf -f -N -L 17443:localhost:16443 k8s-control-1
ssh -F ~/DevProjects/MicroK8s/terraform/development/k8s_ssh.conf -f -N -L 16443:localhost:16443 k8s-control-1
