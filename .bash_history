sudo apt update && sudo apt upgrade -y
sudo apt install python3 python3-pip -y
pip install dvc[gcs]
sudo apt install -y apt-transport-https ca-certificates gnupg
echo "deb https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
sudo apt update && sudo apt install -y google-cloud-sdk
sudo rm /etc/apt/sources.list.d/google-cloud-sdk.list
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo tee /usr/share/keyrings/cloud.google.gpg > /dev/null
sudo apt update && sudo apt install -y google-cloud-sdk
sudo rm /usr/share/keyrings/cloud.google.gpg
curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt update && sudo apt install -y google-cloud-sdk
gcloud --version
gsutil --version
gcloud auth login
gsutil ls gs://data-source-telecom-customers
dvc remote add -d gcp-remote gs://data-source-telecom-customers
dvc --version
pip install dvc
pip install dvc[gcs]
dvc remote add -d gcp-remote gs://data-source-telecom-customers
dvc --version
pip install --user dvc
pip install --user googl
pip install --user google-cloud-storage
~/.local/bin/dvc --version
export PATH=$PATH:~/.local/bin
dvc remote add -d gcp-remote gs://data-source-telecom-customers
dvc init
git init
dvc init
dvc remote add -d gcp-remote gs://data-source-telecom-customers
dvc remote modify gcp-remote credentialpath gs://data-source-telecom-customers/python_modules/axial-rigging-438817-h4-6e7c2d55c995.json
dvc init
dvc remote modify gcp-remote credentialpath data-source-telecom-customers/python_modules/axial-rigging-438817-h4-6e7c2d55c995.json
dvc remote list
dvc add --external gs://data-source-telecom-customers/data/raw_data/train.csv
gsutil cp gs://data-source-telecom-customers/data/raw_data/train.csv .
gsutil cp data-source-telecom-customers/data/raw_data/train
gsutil cp gs://data-source-telecom-customers/data/raw_data/train
gsutil ls gs://data-source-telecom-customers/data/raw_data/
gsutil cp gs://data-source-telecom-customers/data/raw_data/train.csv .
gsutil cp gs://data-source-telecom-customers/data/raw_data/train.csv 
gsutil ls gs://data-source-telecom-customers/data/raw_data/train.csv
gsutil ls gs://data-source-telecom-customers/data/raw_data/
gsutil cp gs://data-source-telecom-customers/data/raw_data/train.csv
dvc status
dvc add data-source-telecom-customers/data/raw_data
ls /home/raagasindhu99/data-source-telecom-customers/data/raw_data
gsutil cp -r gs://data-source-telecom-customers/data/raw_data /home/raagasindhu99/data-source-telecom-customers/data/
gsutil cp -r gs://data-source-telecom-customers/data/raw_data/ /home/raagasindhu99/data-source-telecom-customers/data/
ls /home/raagasindhu99/data-source-telecom-customers/data/raw_data/
mkdir -p /home/raagasindhu99/data-source-telecom-customers/data/raw_data
gsutil cp -r gs://data-source-telecom-customers/data/raw_data/ /home/raagasindhu99/data-source-telecom-customers/data/raw_data/
ls /home/raagasindhu99/data-source-telecom-customers/data/raw_data/
dvc remote add -d gcp-remote gs://data-source-telecom-customers
dvc remote add -d gcp-remote gs://data-source-telecom-customers --force
dvc remote modify gcp-remote credentialpath data-source-telecom-customers/python_modules/axial-rigging-438817-h4-6e7c2d55c995.json
dvc add --to-remote gs://data-source-telecom-customers/data/raw_data/train.csv
pip install dvc[gs]
pip show dvc-gs
dvc add --to-remote gs://data-source-telecom-customers/data/raw_data/train.csv
dvc add --to-remote "gs://data-source-telecom-customers/data/raw_data/train.csv"
dvc status
dvc remote list
dvc add gs://data-source-telecom-customers/data/raw_data/train.csv
gsutil ls gs://data-source-telecom-customers/data/raw_data/
dvc add gs://data-source-telecom-customers/data/raw_data/
dvc import-url gs://data-source-telecom-customers/data/raw_data/ raw_data
git add .gitignore raw_data.dvc
git commit -m "Track raw_data folder from GCS with DVC"
git config --global user.name "raagasindhu99"
git config --global user.email "raagasindhu99@gmail.com"
git commit -m "Track raw_data folder from GCS with DVC"
git push origin main
git status
dvc status
git add raw_data.dvc .gitignore
git commit -m "Track raw_data folder with DVC"
git add raw_data.dvc .gitignore
git status
git add ./raw_data.dvc ./.gitignore
git status
git remote -v
dvc status
ls
dvc status
ls
cd .dvc/tmp
ls
cat dvc.log
dvc push
export GOOGLE_APPLICATION_CREDENTIALS="data-source-telecom-customers/python_modules/axial-rigging-438817-h4-6e7c2d55c995.json"
dvc remote modify gcp-remote credentialpath data-source-telecom-customers/python_modules/axial-rigging-438817-h4-6e7c2d55c995.json
gsutil ls gs://data-source-telecom-customers
dvc push
git clone https://github.com/MLOPS-Team-7/Telecom-Device-Upgrade-Prediction
cd Telecom-Device-Upgrade-Prediction
ls
git config --global user.name "raagasindhu99"
git config --global user.email "raagasindhu99@gmail.com"
echo "Test GCP-GitHub integration" > test.txt
git add test.txt
git commit -m "Testing GitHub connection"
git push origin main
git remote set-url origin https://raagasindhu99:<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>@github.com/MLOPS-Team-7/Telecom-Device-Upgrade-Prediction.git
git remote set-url origin https://raagasindhu99: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@github.com/MLOPS-Team-7/Telecom-Device-Upgrade-Prediction.git
git remote -v
git push origin main
git config --list
git push origin main
git add raw_data.dvc .gitignore
ls
find . -name "raw_data.dvc"
dvc import-url gs://data-source-telecom-customers/data/raw_data/ raw_data
ls
dvc import-url gs://data-source-telecom-customers/data/raw_data/ raw_data
nano .gitignore
git add --force raw_data.dvc
git commit -m "Fix .gitignore and add raw_data.dvc"
git status
git init
git remote -v
git remote add origin "https://github.com/MLOPS-Team-7/Telecom-Device-Upgrade-Prediction"
