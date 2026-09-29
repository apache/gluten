---
layout: page
title: QAT Support in Velox Backend
nav_order: 1
parent: Getting-Started
---

# Intel® QuickAssist Technology (QAT) support

Gluten supports using Intel® QuickAssist Technology (QAT) for data compression during Spark Shuffle. It benefits from QAT Hardware-based acceleration on compression/decompression, and uses Gzip as compression format for higher compression ratio to reduce the pressure on disks and network transmission.

This feature is based on QAT driver library and [QATzip](https://github.com/intel/QATzip) library. Please manually download QAT driver for your system, and follow its README to build and install on all Driver and Worker node: [Intel® QuickAssist Technology Driver for Linux* – HW Version 2.0](https://www.intel.com/content/www/us/en/download/765501/intel-quickassist-technology-driver-for-linux-hw-version-2-0.html?wapkw=quickassist).

## Software Requirements

- Download QAT driver for your system, and follow its README to build and install on all Driver and Worker nodes: [Intel® QuickAssist Technology Driver for Linux* – HW Version 2.0](https://www.intel.com/content/www/us/en/download/765501/intel-quickassist-technology-driver-for-linux-hw-version-2-0.html?wapkw=quickassist).
- Below compression libraries need to be installed on all Driver and Worker nodes:
  - Zlib* library of version 1.2.7 or higher
  - ZSTD* library of version 1.5.4 or higher
  - LZ4* library

## Build Gluten with QAT

1. Setup ICP_ROOT environment variable to the directory where QAT driver is extracted. This environment variable is required during building Gluten and running Spark applications. It's recommended to put it in .bashrc on Driver and Worker nodes.

```bash
echo "export ICP_ROOT=/path/to/QAT_driver" >> ~/.bashrc
source ~/.bashrc

# Also set for root if running as non-root user
sudo su - 
echo "export ICP_ROOT=/path/to/QAT_driver" >> ~/.bashrc
exit
```

2. **This step is required if your application is running as Non-root user.**
   The users must be added to the 'qat' group after QAT drvier is installed. And change the amount of max locked memory for the username that is included in the group name. This can be done by specifying the limit in /etc/security/limits.conf.

```bash
sudo su -
usermod -aG qat username # need relogin to take effect

# To set 500MB add a line like this in /etc/security/limits.conf
echo "@qat - memlock 500000" >> /etc/security/limits.conf

exit
```

3. Enable huge page. This step is required to execute each time after system reboot. We recommend using systemctl to manage at system startup. You change the values for "max_huge_pages" and "max_huge_pages_per_process" to make sure there are enough resources for your workload. As for Spark applications, one process matches one executor. Within the executor, every task is allocated a maximum of 5 huge pages.

```bash
sudo su -

cat << EOF > /usr/local/bin/qat_startup.sh
#!/bin/bash
echo 1024 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
rmmod usdm_drv
insmod $ICP_ROOT/build/usdm_drv.ko max_huge_pages=1024 max_huge_pages_per_process=32
EOF

chmod +x /usr/local/bin/qat_startup.sh

cat << EOF > /etc/systemd/system/qat_startup.service
[Unit]
Description=Configure QAT

[Service]
ExecStart=/usr/local/bin/qat_startup.sh

[Install]
WantedBy=multi-user.target
EOF

systemctl enable qat_startup.service
systemctl start qat_startup.service # setup immediately
systemctl status qat_startup.service

exit
```

4. After the setup, you are now ready to build Gluten with QAT. Use the command below to enable this feature:

```bash
cd /path/to/gluten

## The script builds jars for all supported Spark versions.
./dev/buildbundle-veloxbe.sh --enable_qat=ON
```

## Enable QAT with Gzip/Zstd for shuffle compression

1. To offload shuffle compression into QAT, first make sure you have the right QAT configuration file at /etc/4xxx_devX.conf. We provide a [example configuration file](../qat/4x16.conf). This configuration sets up to 4 processes that can bind to 1 QAT, and each process can use up to 16 QAT DC instances.

```bash
## run as root
## Overwrite QAT configuration file.
cd /etc
for i in {0..7}; do echo "4xxx_dev$i.conf"; done | xargs -i cp -f /path/to/gluten/docs/qat/4x16.conf {}
## Restart QAT after updating configuration files.
adf_ctl restart
```

2. Check QAT status and make sure the status is up

```bash
adf_ctl status
```

The output should be like:

```
Checking status of all devices.
There is 8 QAT acceleration device(s) in the system:
 qat_dev0 - type: 4xxx,  inst_id: 0,  node_id: 0,  bsf: 0000:6b:00.0,  #accel: 1 #engines: 9 state: up
 qat_dev1 - type: 4xxx,  inst_id: 1,  node_id: 1,  bsf: 0000:70:00.0,  #accel: 1 #engines: 9 state: up
 qat_dev2 - type: 4xxx,  inst_id: 2,  node_id: 2,  bsf: 0000:75:00.0,  #accel: 1 #engines: 9 state: up
 qat_dev3 - type: 4xxx,  inst_id: 3,  node_id: 3,  bsf: 0000:7a:00.0,  #accel: 1 #engines: 9 state: up
 qat_dev4 - type: 4xxx,  inst_id: 4,  node_id: 4,  bsf: 0000:e8:00.0,  #accel: 1 #engines: 9 state: up
 qat_dev5 - type: 4xxx,  inst_id: 5,  node_id: 5,  bsf: 0000:ed:00.0,  #accel: 1 #engines: 9 state: up
 qat_dev6 - type: 4xxx,  inst_id: 6,  node_id: 6,  bsf: 0000:f2:00.0,  #accel: 1 #engines: 9 state: up
 qat_dev7 - type: 4xxx,  inst_id: 7,  node_id: 7,  bsf: 0000:f7:00.0,  #accel: 1 #engines: 9 state: up
```

3. Extra Gluten configurations are required when starting Spark application

```
--conf spark.gluten.sql.columnar.shuffle.codec=gzip # Valid options are gzip and zstd
--conf spark.gluten.sql.columnar.shuffle.codecBackend=qat
```

4. You can use below command to check whether QAT is working normally at run-time. The value of fw_counters should continue to increase during shuffle.

```
while :; do cat /sys/kernel/debug/qat_4xxx_0000:6b:00.0/fw_counters; sleep 1; done
```

## QAT driver references

**Documentation**

[README Text Files (README_QAT20.L.1.0.0-00021.txt)](https://downloadmirror.intel.com/765523/README_QAT20.L.1.0.0-00021.txt)

**Release Notes**

Check out the [Intel® QuickAssist Technology Software for Linux*](https://www.intel.com/content/www/us/en/content-details/632507/intel-quickassist-technology-intel-qat-software-for-linux-release-notes-hardware-version-2-0.html) - Release Notes for the latest changes in this release.

**Getting Started Guide**

Check out the [Intel® QuickAssist Technology Software for Linux*](https://www.intel.com/content/www/us/en/content-details/632506/intel-quickassist-technology-intel-qat-software-for-linux-getting-started-guide-hardware-version-2-0.html) - Getting Started Guide for detailed installation instructions.

**Programmer's Guide**

Check out the [Intel® QuickAssist Technology Software for Linux*](https://www.intel.com/content/www/us/en/content-details/743912/intel-quickassist-technology-intel-qat-software-for-linux-programmers-guide-hardware-version-2-0.html) - Programmer's Guide for software usage guidelines.

For more Intel® QuickAssist Technology resources go to [Intel® QuickAssist Technology (Intel® QAT)](https://developer.intel.com/quickassist)
