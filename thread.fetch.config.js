module.exports = {
  apps: [
    {
      name: "investing_fetch",
      interpreter: "python3",
      script: "./Investing/core/thread_fetch.py",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
