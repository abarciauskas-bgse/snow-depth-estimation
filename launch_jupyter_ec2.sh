#!/bin/bash

# EC2 Launch Script with JupyterLab User Data
# Usage: ./launch_jupyter_ec2.sh

# Variables - Modify these as needed
VPC_ID="vpc-049bdcfd473c6af56"
KEY_NAME="barciaai"
INSTANCE_TYPE="r7i.large"
AMI_ID="ami-01102c5e8ab69fb75"
YOUR_IP=echo $(curl -s https://api.ipify.org)/32
INSTANCE_NAME="aimees-jupyter"

# Create user data file
cat > user-data.sh << 'EOF'
#!/bin/bash
# User data script for Ubuntu EC2 instance with JupyterLab remote access

# Install Python and pip
sudo dnf install -y python3.12 python3.12-pip git

# Create a virtual environment
sudo -u ec2-user python3.12 -m venv /home/ec2-user/jupyter-env

# Activate it
source ~/jupyter-env/bin/activate

# Install jupyterlab in the virtual environment
pip3.12 install jupyterlab notebook

# Test it
jupyter lab --version

# Generate Jupyter config
sudo -u ec2-user jupyter lab --generate-config

# Create Jupyter config directory if it doesn't exist
mkdir -p /home/ec2-user/.jupyter
chown ec2-user:ec2-user /home/ec2-user/.jupyter

# Configure JupyterLab for remote access
cat > /home/ec2-user/.jupyter/jupyter_lab_config.py << EOL
c.ServerApp.ip = '127.0.0.1'
c.ServerApp.port = 8888
c.ServerApp.open_browser = False
c.ServerApp.allow_root = True
c.ServerApp.allow_remote_access = True
c.ServerApp.token = ''
EOL

# Set correct ownership
chown ec2-user:ec2-user /home/ec2-user/.jupyter/jupyter_lab_config.py

# Install common data science packages
pip3.12 install boto3 aiobotocore pandas numpy matplotlib scikit-learn geopandas \
  rasterio rioxarray pyarrow shapely xarray pyproj folium fsspec requests \
  plotly earthaccess pystac joblib pystac-client jupyter-resource-usage \
  cartopy

# Create systemd service for JupyterLab
cat > /etc/systemd/system/jupyterlab.service << EOL
[Unit]
Description=JupyterLab Server
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user
ExecStart=/usr/local/bin/jupyter lab
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOL

# Enable and start the service
systemctl daemon-reload
systemctl enable jupyterlab
systemctl start jupyterlab

# Set up firewall (optional)
ufw allow 8888/tcp
ufw --force enable

# Log completion
echo "JupyterLab setup completed at $(date)" >> /var/log/jupyter-setup.log
EOF

echo "Creating security group..."
SECURITY_GROUP_ID=$(aws ec2 create-security-group \
    --group-name "jupyter-sg-$(date +%s)" \
    --description "Security group for JupyterLab access" \
    --vpc-id "$VPC_ID" \
    --output text --query 'GroupId')

echo "Security Group ID: $SECURITY_GROUP_ID"

echo "Authorizing SSH access..."
aws ec2 authorize-security-group-ingress \
    --group-id "$SECURITY_GROUP_ID" \
    --protocol tcp \
    --port 22 \
    --cidr "$YOUR_IP"

echo "Launching EC2 instance..."
INSTANCE_ID=$(aws ec2 run-instances \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --user-data file://user-data.sh \
    --security-group-ids "$SECURITY_GROUP_ID" \
    --associate-public-ip-address \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$INSTANCE_NAME}]" \
    --metadata-options "HttpEndpoint=enabled,HttpPutResponseHopLimit=2,HttpTokens=required" \
    --iam-instance-profile Name=jupyter-ec2-role \
    --output text --query 'Instances[0].InstanceId')

echo "Instance ID: $INSTANCE_ID"

echo "Waiting for instance to be running..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"

echo "Getting public IP address..."
PUBLIC_IP=$(aws ec2 describe-instances \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' \
    --output text)

echo ""
echo "========================================="
echo "EC2 Instance launched successfully!"
echo "========================================="
echo "Instance ID: $INSTANCE_ID"
echo "Security Group ID: $SECURITY_GROUP_ID"
echo "Public IP: $PUBLIC_IP"
echo ""
echo "To check setup progress:"
echo "ssh -i ~/.ssh/$KEY_NAME.pem ec2-user@$PUBLIC_IP 'tail -f /var/log/cloud-init-output.log'"
echo ""
echo "To check JupyterLab service status:"
echo "ssh -i ~/.ssh/$KEY_NAME.pem ec2-user@$PUBLIC_IP 'sudo systemctl status jupyterlab'"
echo ""

# Clean up user data file
rm -f user-data.sh

echo "Setup complete! User data file has been cleaned up."
