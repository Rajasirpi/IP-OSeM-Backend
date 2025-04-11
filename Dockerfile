FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/
RUN pip install -r requirements.txt

COPY . /app/

EXPOSE 8000

# Install cron in the container
RUN apt-get update && apt-get install -y cron

# RUN echo "#!/bin/bash" > /etc/cron.d/fetch_bike_data && \
#     printf "0 */6 * * * /usr/local/bin/python /app/manage.py fetch_bike_data ms >> /proc/1/fd/1 2>&1\n" >> /etc/cron.d/fetch_bike_data && \
#     printf "0 */6 * * * * /usr/local/bin/python /app/manage.py fetch_bike_data os >> /proc/1/fd/1 2>&1\n" >> /etc/cron.d/fetch_bike_data

RUN echo "*/10 * * * * bash -c '/usr/local/bin/python /app/manage.py fetch_bike_data ms && /usr/local/bin/python /app/manage.py fetch_bike_data os' >> /proc/1/fd/1 2>&1\n" >> /etc/cron.d/fetch_bike_data

# # Copy the crontab file if you want to create a custom setup
# RUN echo "*/5 * * * * root /usr/local/bin/python /app/manage.py fetch_bike_data ms" >> /etc/cron.d/fetch_bike_data
# RUN echo "*/5 * * * * root /usr/local/bin/python /app/manage.py fetch_bike_data os" >> /etc/cron.d/fetch_bike_data
# # RUN echo "0 */6 * * *  /usr/local/bin/python /app/manage.py fetch_bike_data os >> /var/log/cron.log 2>&1" >> /etc/cron.d/fetch_bike_data

# Set the permissions on the cron job
RUN chmod 0644 /etc/cron.d/fetch_bike_data

# Apply the cron job
RUN crontab /etc/cron.d/fetch_bike_data

# Create the log file to be able to run the `cron` command
RUN touch /var/log/cron.log

# # Run cron in the foreground
# CMD ["cron", "-f"]
CMD ["/bin/bash", "-c", "/usr/local/bin/python /app/manage.py fetch_bike_data ms && /usr/local/bin/python /app/manage.py fetch_bike_data os && cron -f"]

