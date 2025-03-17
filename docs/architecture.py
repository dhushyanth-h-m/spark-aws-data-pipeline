#!/usr/bin/env python3
"""
Generate a simple architecture diagram for the NYC Taxi Data Pipeline.
This script creates a PNG image showing the high-level architecture.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
import os

# Create figure and axis
fig, ax = plt.subplots(figsize=(12, 8))

# Define component positions
components = {
    "NYC Taxi Data": (0.1, 0.7),
    "S3 Raw": (0.3, 0.7),
    "EMR Cluster": (0.6, 0.7),
    "S3 Processed": (0.3, 0.3),
    "Analytics": (0.1, 0.3),
    "Airflow": (0.6, 0.3)
}

# Define component sizes
width, height = 0.15, 0.1

# Define arrows
arrows = [
    ("NYC Taxi Data", "S3 Raw"),
    ("S3 Raw", "EMR Cluster"),
    ("EMR Cluster", "S3 Processed"),
    ("S3 Processed", "Analytics"),
    ("Airflow", "EMR Cluster"),
    ("Airflow", "S3 Raw")
]

# Draw components
for name, (x, y) in components.items():
    rect = patches.Rectangle((x, y), width, height, linewidth=1, edgecolor='black', facecolor='lightblue')
    ax.add_patch(rect)
    ax.text(x + width/2, y + height/2, name, ha='center', va='center')

# Draw arrows
for start, end in arrows:
    start_x, start_y = components[start]
    end_x, end_y = components[end]
    
    # Adjust start and end points to be at the edge of the boxes
    if start_x < end_x:
        start_x += width
    else:
        end_x += width
        
    if start_y < end_y:
        start_y += height
    else:
        end_y += height
    
    ax.arrow(start_x, start_y, end_x - start_x, end_y - start_y, 
             head_width=0.02, head_length=0.02, fc='black', ec='black')

# Set axis limits and remove ticks
ax.set_xlim(0, 1)
ax.set_ylim(0, 1)
ax.set_xticks([])
ax.set_yticks([])

# Set title
ax.set_title('NYC Taxi Data Pipeline Architecture')

# Save the figure
os.makedirs('docs', exist_ok=True)
plt.savefig('docs/architecture.png', dpi=300, bbox_inches='tight')
plt.close()

print("Architecture diagram saved to docs/architecture.png") 