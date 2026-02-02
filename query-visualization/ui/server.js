import express from "express";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json({ limit: "5mb" }));

// Static UI
app.use(express.static(path.join(__dirname, "public")));

// Proxy to Python API
const PY_API_URL = process.env.PY_API_URL || "http://localhost:8080/visualize";

app.post("/visualize", async (req, res) => {
  try {
    const resp = await fetch(PY_API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req.body),
    });
    const data = await resp.json();
    res.status(resp.status).json(data);
  } catch (err) {
    res.status(500).json({ error: String(err) });
  }
});

app.listen(3001, () => {
  console.log("UI server running on http://localhost:3001");
});
