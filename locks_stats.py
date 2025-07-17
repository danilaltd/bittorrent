import re
import os
import pandas as pd

# Read log file
with open(os.path.join('logs', 'locks.log'), 'r') as f:
    lines = f.readlines()

# Parse lines
data = []
pattern = re.compile(r'\[.*?\]\[LOCK\] Waiting ([0-9.]+)s for ([^.]+)\.(read_access|write_access) \(thread: ([^)]+)\)')
for line in lines:
    match = pattern.search(line)
    if match:
        time_wait = float(match.group(1))
        lock_name = match.group(2)
        access_type = match.group(3).replace('_access', '')
        thread = match.group(4)
        data.append((lock_name, access_type, thread, time_wait))

# Create DataFrame
df = pd.DataFrame(data, columns=['lock_name', 'access_type', 'thread', 'time_wait'])

# Compute metrics
group = df.groupby('lock_name')
summary = group.agg(
    total_time=('time_wait', 'sum'),
    count=('time_wait', 'count'),
    avg_time=('time_wait', 'mean')
).reset_index()

# Read/write breakdown
rw = df.groupby(['lock_name', 'access_type']).agg(
    rw_count=('time_wait', 'count'),
    rw_time=('time_wait', 'sum')
).unstack(fill_value=0)
# Flatten multiindex
rw.columns = [f"{atype}_{metric}" for metric, atype in rw.columns]
rw = rw.reset_index()

# Thread stats: number of threads interacting
threads = df.groupby('lock_name').thread.nunique().reset_index().rename(columns={'thread': 'unique_threads'})

# Merge all
summary = summary.merge(rw, on='lock_name').merge(threads, on='lock_name')

# Sort by total_time and count
by_time = summary.sort_values('total_time', ascending=False)
by_count = summary.sort_values('count', ascending=False)

# Display top 10 each
top_time = by_time.head(10)
top_count = by_count.head(10)

print("Top Locks by Total Waiting Time", top_time)
print("Top Locks by Call Count", top_count)
