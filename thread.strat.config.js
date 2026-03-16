module.exports = {
  apps: [
    {
      name: "investing_strat",
      interpreter: "python3",
      script: "./Investing/core/thread_strat.py",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
