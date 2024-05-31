require('dotenv').config();
const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');
const async = require('async');
const util = require('util');

AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION,
  httpOptions: {
    timeout: 166400000,
    connectTimeout: 120000
  }
});

const parseBoolean = (value) => {
  return value === 'true' || value === '1';
};

const s3Config = {
  useAccelerateEndpoint: parseBoolean(process.env.S3_TRANSFER_ACCELERATION)
};

const s3 = new AWS.S3(s3Config);

const readdir = util.promisify(fs.readdir);
const stat = util.promisify(fs.stat);

const uploadFileToS3 = async (filePath, key) => {
  const fileStream = fs.createReadStream(filePath);
  const uploadParams = {
    Bucket: process.env.S3_BUCKET_NAME,
    Key: key,
    Body: fileStream,
    ACL: 'private',
    ContentDisposition: 'inline',
  };
  try {
    const data = await s3.upload(uploadParams).promise();
    return data.Location;
  } catch (err) {
    throw err;
  }
};

const multipartUploadFileToS3 = async (filePath, key) => {
  const fileStream = fs.createReadStream(filePath);
  const uploadParams = {
    Bucket: process.env.S3_BUCKET_NAME,
    Key: key,
    Body: fileStream,
    ACL: 'private',
    ContentDisposition: 'inline',
  };
  const partSize = 3 * 1024 * 1024; // 3MB
  try {
    const data = await s3.upload(uploadParams, {
      partSize: partSize,
      queueSize: parseInt(process.env.QUEUE_SIZE) || 20, // Parallel upload parts
    }).promise();
    return data.Location;
  } catch (err) {
    throw err;
  }
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
      parseInt(process.env.CONCURRENCY) || 20,
      async (file) => {
        const relativePath = path.relative(dir, file);
        const s3Key = path.join('', relativePath).replace(/\\/g, '/');
        let retries = 3;
        while (retries > 0) {
          try {
            if (fs.statSync(file).size > 3 * 1024 * 1024) { // greater then 3 MB
              await multipartUploadFileToS3(file, s3Key);
            } else {
              await uploadFileToS3(file, s3Key);
            }
            console.log(`Uploaded: ${s3Key}`);
            break;
          } catch (err) {
            console.error(`Failed to upload ${s3Key}:`, err);
            retries -= 1;
            if (retries === 0) {
              console.error(`Giving up on ${s3Key}`);
            } else {
              console.log(`Retrying ${s3Key} (${3 - retries}/3)`);
            }
          }
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
