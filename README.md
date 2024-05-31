# Steps to run:-

1. npm i , copy all from .env-sample and create a file .env and paste it there along with your access and secret keys
2. Delete previously present example folders and resume from uploads folder
3. Upload all folders in the uploads folder , have it arranged in similar way that you want in bucket
4. For example have folders 2020, 2021, 2022 etc inside that and its subfolders will have all resume as wanted im bucket
5. Run - 'npm start' or 'npm run batch-upload'


# Enable Transfer Acceleration on Your S3 Bucket for fast transfers (Charges - 0.04$/GB):

1. Go to the S3 console.
2. Select your bucket.
3. Go to the "Properties" tab.
4. Under "Transfer Acceleration," click "Edit" and enable it.
5. You will see the s3 link for transfer acceleration, click on save changes.


## Credits - @yashpritwani