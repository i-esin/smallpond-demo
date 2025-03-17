import smallpond


sp = smallpond.init()

print(f"""
    Usable CPU count: {sp.runtime_ctx.usable_cpu_count}
    Usable GPU count: {sp.runtime_ctx.usable_gpu_count}
    Total RAM: {sp.runtime_ctx.total_memory/(1024*1024*1024)} GB
    Usable RAM size: {sp.runtime_ctx.usable_memory_size/(1024*1024*1024)} GB
    Available RAM: {round(sp.runtime_ctx.available_memory/(1024*1024*1024), 2)} GB
""")