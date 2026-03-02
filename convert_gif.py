from PIL import Image
import sys

webp_path = r"C:\Users\Ragotham Kanchi\.gemini\antigravity\brain\b432cc16-a490-43f3-95b3-d333c3ade10a\dataflow_animation_demo_1772437620785.webp"
gif_path = r"C:\Users\Ragotham Kanchi\OneDrive\Documents\Data Engineering\endpoint-inventory-pipeline\portfolio\dataflow_animation.gif"

try:
    im = Image.open(webp_path)
    frames = []
    try:
        while True:
            # Convert to RGB to ensure GIF compatibility
            frame = im.convert('RGB')
            frames.append(frame)
            im.seek(len(frames))
    except EOFError:
        pass
        
    dur = im.info.get('duration', 100)
    if not isinstance(dur, int) or dur == 0:
        dur = 100 # Default to 10fps
        
    frames[0].save(gif_path, save_all=True, append_images=frames[1:], duration=dur, loop=0)
    print("Successfully converted WebP to GIF at: " + gif_path)
except Exception as e:
    import traceback
    traceback.print_exc()
    print(f"Error: {e}")
