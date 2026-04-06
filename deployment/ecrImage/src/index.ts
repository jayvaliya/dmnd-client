import { ImageManagement } from './image';

const version = process.env.VERSION;
if (!version) {
    throw new Error('VERSION environment variable is not set. Please set it to the desired release tag.');
}

const image = new ImageManagement('dmnd-client-image', {
  appName: 'client',
  dockerContext: '../../',
  dockerfile: '../../Dockerfile',
  imageTag: version,
});

export { image };
