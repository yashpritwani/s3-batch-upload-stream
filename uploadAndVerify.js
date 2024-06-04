require('dotenv').config();
const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
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

const calculateMD5 = (filePath) => {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('md5');
    const stream = fs.createReadStream(filePath);

    stream.on('data', (data) => hash.update(data));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', (err) => reject(err));
  });
};

const calculateMultipartMD5 = (filePath, partSize) => {
  return new Promise((resolve, reject) => {
    const parts = [];
    const readStream = fs.createReadStream(filePath, { highWaterMark: partSize });
    
    readStream.on('data', (chunk) => {
      const hash = crypto.createHash('md5').update(chunk).digest('hex');
      parts.push(hash);
    });

    readStream.on('end', () => {
      const multipartHash = crypto.createHash('md5').update(Buffer.from(parts.join(''), 'hex')).digest('hex');
      resolve(multipartHash);
    });

    readStream.on('error', (err) => reject(err));
  });
};

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
    return data;
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
  const partSize = 3 * 1024 * 1024; // 5MB
  try {
    const data = await s3.upload(uploadParams, {
      partSize: partSize,
      queueSize: parseInt(process.env.QUEUE_SIZE) || 20, // Parallel upload parts
    }).promise();
    return data;
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

const uploadAndVerifyDirectory = async (dir) => {
  try {
    const files = await walkDirectory(dir);
    console.log(`Found ${files.length} files to upload and verify.`);

    for (const file of files) {
      const relativePath = path.relative(dir, file);
      const s3Key = path.join('', relativePath).replace(/\\/g, '/');
      const fileSize = (await stat(file)).size;

      let localMD5;
      if (fileSize > 3 * 1024 * 1024) {
        // For multipart uploads, calculate the MD5 differently
        localMD5 = await calculateMultipartMD5(file, 3 * 1024 * 1024);
      } else {
        localMD5 = await calculateMD5(file);
      }
      console.log(`Calculated MD5 for ${file}: ${localMD5}`);

      const retries = 3;
      for (let attempt = 1; attempt <= retries; attempt++) {
        try {
          const data = fileSize > 3 * 1024 * 1024 ? 
                       await multipartUploadFileToS3(file, s3Key) : 
                       await uploadFileToS3(file, s3Key);
          const s3ETag = data.ETag.replace(/"/g, '');

          if (localMD5 === s3ETag) {
            console.log(`Uploaded and verified: ${s3Key}`);
          } else {
            console.error(`MD5 mismatch for ${s3Key}. Local: ${localMD5}, S3: ${s3ETag}`);
          }
          break;
        } catch (err) {
          console.error(`Failed to upload ${s3Key} on attempt ${attempt}:`, err);
          if (attempt === retries) {
            console.error(`Giving up on ${s3Key} after ${retries} attempts`);
          }
        }
      }
    }
  } catch (err) {
    console.error('Error processing directory:', err);
  }
};

const main = async () => {
  const directoryToUpload = process.argv[2];
  if (!directoryToUpload) {
    console.log('Please provide a directory to upload.');
    process.exit(1);
  }
  await uploadAndVerifyDirectory(directoryToUpload);
};

main();
