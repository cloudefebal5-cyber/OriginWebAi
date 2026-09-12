// /api/health.js
module.exports = function handler(req, res) {
  const providers = {
    claude: !!process.env.ANTHROPIC_API_KEY,
    openai: !!process.env.OPENAI_API_KEY,
    gemini: !!process.env.GEMINI_API_KEY,
  };
  const anyConfigured = Object.values(providers).some(Boolean);
  res.status(200).json({ ok: anyConfigured, providers });
};
