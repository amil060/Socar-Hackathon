from flask import Flask, render_template, request
import subprocess
import time

app = Flask(__name__)

PIPELINE_SCRIPT = "/home/hackathon/webapp/scripts/run_pipeline.sh"

@app.route("/", methods=["GET", "POST"])
def index():
    message = ""
    if request.method == "POST":
        data_dir = request.form.get("data_dir")
        run_id = f"web_{int(time.time())}"

        subprocess.Popen(
            [PIPELINE_SCRIPT, data_dir, run_id],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        message = f"Pipeline started with RUN_ID: {run_id}"

    return render_template("index.html", message=message)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
