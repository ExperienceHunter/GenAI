const express = require('express');
const swaggerUi = require('swagger-ui-express');
const YAML = require('yamljs');
const userRoutes = require('./routes/user');
const elasticsearchRoutes = require('./routes/elasticsearch'); // Import Elasticsearch routes

const app = express();
app.use(express.json());

// Load Swagger YAML file for documentation
const swaggerDocument = YAML.load('./docs/swagger.yaml');
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument));

// Use routes
app.use('/users', userRoutes);
app.use('/elasticsearch', elasticsearchRoutes);

// Start the server
app.listen(3000, () => {
  console.log('Server running at http://localhost:3000');
  console.log('Swagger docs at http://localhost:3000/api-docs');
});
