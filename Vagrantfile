Vagrant.configure("2") do |config|
  config.vm.box = "lpf/centos7docker"
  config.vm.network "private_network", ip: "172.30.1.6"
  config.vm.provider :virtualbox do |p|
	p.memory=4096
	p.cpus=4
  end
  config.vm.provision "file", source: "docker-compose.yml", destination: "docker-compose.yml"
  config.vm.provision "shell",
    privileged: false,
    inline: "docker-compose up -d"
end