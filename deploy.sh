#!/bin/bash
# scp ./nifi-cdp-nar/target/nifi-cdp-1.0.nar dev@laptop_wsl:/opt/nifi/lib/
cp ./nifi-cdp-nar/target/nifi-cdp-1.0.nar /opt/nifi/lib/
echo Deployment done.
echo NiFi Service will berestarted.
sudo systemctl restart nifi.service
echo Restart done.
