# Quick Start Guide - Healthcare Analytics Platform

## 🚀 Getting Started in 5 Steps

### Step 1: Get Your CSV Files ✅

**You need these 3 files first:**
- `ipps_charges.csv` (~146K rows, 40MB)
- `hospital_general_info.csv` (~5.4K rows, 1MB)  
- `readmissions.csv` (~18.5K rows, 2MB)

**Where to get them:**
- CMS public datasets website
- Download and save to: `data/` folder in this project

**Create the folder:**
```bash
mkdir data
# Then place your CSV files there
```

---

### Step 2: Set Up AWS (For Phase 1A) ☁️

**You need:**
- AWS account (free tier works!)
- AWS Access Key ID
- AWS Secret Access Key

**How to get credentials:**
1. Go to https://aws.amazon.com → Sign up (free)
2. Go to IAM → Users → Create User
3. Select "Programmatic access"
4. Attach policy: `AmazonS3FullAccess`
5. **Save the Access Key ID and Secret Key** (you can't see secret again!)

**Configure locally:**
```bash
aws configure
# Enter your Access Key ID
# Enter your Secret Access Key
# Enter region: us-east-1
# Enter output: json
```

**Test it:**
```bash
aws sts get-caller-identity
# Should show your account info
```

📖 **Detailed guide:** See `docs/prerequisites_and_credentials.md`

---

### Step 3: Upload Files to S3 📤

**Install Python dependencies:**
```bash
pip install -r scripts/requirements.txt
```

**Run upload script:**
```bash
python scripts/s3_upload.py
```

**What it does:**
- Creates S3 bucket structure
- Uploads your 3 CSV files
- Adds metadata and encryption
- Creates upload manifest

**Expected output:**
```
✓ Successfully uploaded ipps_charges.csv
✓ Successfully uploaded hospital_general_info.csv
✓ Successfully uploaded readmissions.csv
```

---

### Step 4: Set Up Snowflake (For Phase 1B) ❄️

**You need:**
- Snowflake account (free trial: https://signup.snowflake.com)
- Account locator (from Snowflake URL)
- Username and password

**How to get:**
1. Sign up at https://signup.snowflake.com (30-day free trial, $400 credit)
2. Choose: Standard edition, AWS cloud, US East region
3. Log in and note your account locator from URL
4. Create a user (or use default)
5. Save credentials securely

**We'll configure this in Phase 1B** - no action needed now!

---

### Step 5: Run Phase 1A Scripts 🎯

**Create S3 bucket:**
```bash
bash scripts/setup_s3_bucket.sh
```

**Upload files:**
```bash
python scripts/s3_upload.py
```

**Verify upload:**
```bash
aws s3 ls s3://healthcare-analytics-datalake/bronze/raw/cms-data/ --recursive
```

---

## 📋 Checklist

### Before Phase 1A:
- [ ] Have 3 CSV files downloaded
- [ ] Created `data/` folder with CSV files
- [ ] Have AWS account
- [ ] Created IAM user with S3 access
- [ ] Saved AWS credentials securely
- [ ] Ran `aws configure`
- [ ] Tested AWS connection

### Before Phase 1B:
- [ ] Completed Phase 1A (files in S3)
- [ ] Have Snowflake account
- [ ] Saved Snowflake credentials
- [ ] Ready to run SQL setup scripts

---

## 🔒 Security Reminders

✅ **DO:**
- Configure credentials locally
- Use environment variables
- Keep credentials in password manager
- Test connections before proceeding

❌ **DON'T:**
- Share credentials with anyone
- Commit credentials to git
- Use root AWS account
- Hardcode credentials in scripts

---

## ❓ Common Questions

**Q: Do I need to pay for AWS/Snowflake?**
A: No! Both have free tiers that cover this project:
- AWS S3: 5GB free/month (we use ~43MB)
- Snowflake: 30-day trial with $400 credit

**Q: Can I skip S3 and load directly to Snowflake?**
A: Yes, but S3 is part of the data lake architecture we're demonstrating. You can load CSVs directly to Snowflake if preferred.

**Q: What if I don't have the CSV files yet?**
A: Download them from CMS first, then proceed. The scripts won't work without the files.

**Q: Do I need to share my credentials with you?**
A: **NO!** Never share credentials. Configure them locally using `aws configure` or environment variables.

---

## 🆘 Need Help?

1. **Check error messages** - they usually tell you what's wrong
2. **Verify credentials** - make sure they're correct
3. **Read detailed docs:**
   - `docs/prerequisites_and_credentials.md` - Full credential setup
   - `docs/s3_setup_instructions.md` - S3 setup details
   - `docs/data_lake_architecture.md` - Architecture overview

4. **Ask me questions** (without sharing credentials!)

---

## 🎯 Next Steps

Once you have:
- ✅ CSV files in `data/` folder
- ✅ AWS credentials configured
- ✅ S3 bucket created and files uploaded

Then proceed to:
- **Phase 1B**: Snowflake setup and data loading
- **Phase 1C**: Data quality validation

