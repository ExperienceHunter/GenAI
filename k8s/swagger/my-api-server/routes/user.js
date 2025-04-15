const express = require('express');
const router = express.Router();

// Sample users data for the sake of example
let users = [
  { id: 1, name: 'John Doe' },
  { id: 2, name: 'Jane Doe' },
];

router.get('/', (req, res) => {
  res.json(users);
});

router.post('/', (req, res) => {
  const { name } = req.body;
  const newUser = { id: Date.now(), name };
  users.push(newUser);
  res.status(201).json(newUser);
});

module.exports = router;
