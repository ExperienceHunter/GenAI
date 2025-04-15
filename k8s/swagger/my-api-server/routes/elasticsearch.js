const express = require('express');
const { Client } = require('@elastic/elasticsearch');
const router = express.Router();

// Create an Elasticsearch client instance
const client = new Client({ node: 'http://localhost:9200' });

// Search documents in Elasticsearch using full request body
router.post('/search', async (req, res) => {
  const { index = 'document-embeddings', body = {} } = req.body;

  try {
    const result = await client.search({
      index,
      body
    });

    res.json(result.hits.hits);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Search failed' });
  }
});

// Index a new document in Elasticsearch
router.post('/index', async (req, res) => {
  const { id, content, index = 'document-embeddings' } = req.body;

  try {
    const result = await client.index({
      index,
      id,
      document: { content }
    });

    res.status(201).json(result);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Indexing failed' });
  }
});

module.exports = router;
