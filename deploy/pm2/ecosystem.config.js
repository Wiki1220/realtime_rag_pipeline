module.exports = {
  apps: [
    {
      name: "realtime-rag-api",
      cwd: "/data/realtime_rag_pipeline",
      script: "/home/wuteng/.conda/envs/realtime-rag/bin/python",
      args: "main.py serve --config configs/pipeline_preset5.yaml --host 0.0.0.0 --port 8011",
      autorestart: true,
      max_restarts: 20,
      restart_delay: 5000,
      kill_timeout: 10000,
      env: {
        PYTHONUNBUFFERED: "1",
      },
    },
    {
      name: "realtime-rag-worker",
      cwd: "/data/realtime_rag_pipeline",
      script: "/home/wuteng/.conda/envs/realtime-rag/bin/python",
      args: "main.py worker --config configs/pipeline_preset5.yaml",
      autorestart: true,
      max_restarts: 20,
      restart_delay: 5000,
      kill_timeout: 10000,
      env: {
        PYTHONUNBUFFERED: "1",
      },
    },
  ],
};
