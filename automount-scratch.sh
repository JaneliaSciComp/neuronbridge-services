sudo yum -y update
sudo yum install -y fuse-devel
sudo mkfs -t ext4 /dev/xvdb
sudo mkdir /scratch_volume
sudo echo -e '/dev/xvdb\t/scratch_volume\text4\tdefaults\t0\t0' | sudo tee -a /etc/fstab
sudo mount –a
sudo stop ecs
sudo rm -rf /var/lib/ecs/data/ecs_agent_data.json
