with open("ai_filter.py", "r") as f:
    ai_code = f.read()

# Fix 1a: Context joining in extract_feed_summary_parts
ai_code = ai_code.replace('context = " ".join(lines[1:]).strip()', 'context = "\\n".join(lines[1:]).strip()')

# Fix 1b: Story newline preservation
ai_code = ai_code.replace('resolved_story = normalize_space(narrative_story)', 'resolved_story = str(narrative_story or "").strip()')

# Fix 2: Add skip rule for geographic background
ai_code = ai_code.replace(
    '"Use skip for casualty reports (e.g., \'soldiers killed\') that fail to specify whose soldiers or which faction they belong to.\\n"',
    '"Use skip for casualty reports (e.g., \'soldiers killed\') that fail to specify whose soldiers or which faction they belong to.\\n"\n        "Use skip for purely informational, geographical, or background sentences (e.g. \'City X is home to Base Y\') that do not contain a concrete new event or development.\\n"'
)

with open("ai_filter.py", "w") as f:
    f.write(ai_code)

with open("prompts.py", "r") as f:
    p_code = f.read()

# Fix 3: Sports scores contextualization
p_code = p_code.replace(
    '7f) STORY MERGING: When multiple posts cover the same incident or closely related events, merge them into a single stronger, more specific line that captures the key facts. Do not repeat the same event in multiple weak lines.',
    '7f) STORY MERGING: When multiple posts cover the same incident or closely related events, merge them into a single stronger, more specific line that captures the key facts. Do not repeat the same event in multiple weak lines.\n7g) SPORTS SCORES: When reporting sports scores, always include the sport and contextualize the numbers (e.g., "won in sets of 6-7, 7-6" rather than just "6:7 7:6").'
)

with open("prompts.py", "w") as f:
    f.write(p_code)

print("Patched.")
