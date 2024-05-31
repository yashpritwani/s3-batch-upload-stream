require('dotenv').config();
const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');
const async = require('async');
const util = require('util');
const s3 = new AWS.S3();
const readdir = util.promisify(fs.readdir);
const stat = util.promisify(fs.stat);

AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION
});

const uploadFileToS3 = (filePath, key) => {
  return new Promise((resolve, reject) => {
    const fileStream = fs.createReadStream(filePath);
    fileStream.on('error', (err) => {
      reject(err);
    });
    const uploadParams = {
      Bucket: process.env.S3_BUCKET_NAME,
      Key: key,
      Body: fileStream,
      ACL: 'private',
      ContentDisposition: 'inline',
    };
    s3.upload(uploadParams, (err, data) => {
      if (err) {
        reject(err);
      } else {
        resolve(data.Location); // Resolve with the file location
      }
    });
  });
};

const walkDirectory = async (dir) => {
  let files = [];
  const items = await readdir(dir);
  for (const item of items) {
    const fullPath = path.join(dir, item);
    const stats = await stat(fullPath);
    if (stats.isDirectory()) {
      files = files.concat(await walkDirectory(fullPath));
    } else if (stats.isFile()) {
      files.push(fullPath);
    }
  }
  return files;
};

const uploadDirectory = async (dir) => {
  try {
    const files = await walkDirectory(dir);
    console.log(`Found ${files.length} files to upload.`);
    async.eachLimit(
      files,
      20,
      async (file) => {
        const relativePath = path.relative(dir, file);
        const s3Key = path.join('', relativePath).replace(/\\/g, '/');
        try {
          await uploadFileToS3(file, s3Key);
          console.log(`Uploaded: ${s3Key}`);
        } catch (err) {
          console.error(`Failed to upload ${s3Key}:`, err);
        }
      },
      (err) => {
        if (err) {
          console.error('Failed to upload files:', err);
        } else {
          console.log('All files uploaded successfully.');
        }
      }
    );
  } catch (err) {
    console.error('Error reading directory:', err);
  }
};

const main = async () => {
  const directoryToUpload = process.argv[2];
  if (!directoryToUpload) {
    console.log('Please provide a directory to upload.');
    process.exit(1);
  }
  await uploadDirectory(directoryToUpload);
};

main();