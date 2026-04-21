import ast

def calculate_absolute_score(absolute_difference):
    low, high = 0.3, 0.5
    if absolute_difference <= low: return 0.0
    if absolute_difference >= high: return 1.0
    return (absolute_difference - low) / (high - low)

def calculate_absolute(current_strategy, checkpoint_strategy):
    # Convert to dict if they are strings
    if isinstance(current_strategy, str):
        current_strategy = ast.literal_eval(current_strategy)
    if isinstance(checkpoint_strategy, str):
        checkpoint_strategy = ast.literal_eval(checkpoint_strategy)

    total_difference = 0.0
    
    # Check netuid 1 through 128
    for netuid in range(1, 129):  # 1 to 128
        
        # Get the value from current strategy, or 0 if it doesn't exist
        if netuid in current_strategy:
            current_value = current_strategy[netuid]
        else:
            current_value = 0.0
        
        # Get the value from checkpoint strategy, or 0 if it doesn't exist
        if netuid in checkpoint_strategy:
            checkpoint_value = checkpoint_strategy[netuid]
        else:
            checkpoint_value = 0.0
        
        # Calculate how different these two numbers are
        difference = abs(current_value - checkpoint_value)
        
        # Add to running total
        total_difference = total_difference + difference
    
    # Return the final sum
    return total_difference

def calculate_top15_score(current_strategy, checkpoint_strategy, top_n=15):
    # Convert to dict if they are strings
    if isinstance(current_strategy, str):
        current_strategy = ast.literal_eval(current_strategy)
    if isinstance(checkpoint_strategy, str):
        checkpoint_strategy = ast.literal_eval(checkpoint_strategy)
    
    # Get top N subnets from checkpoint (sorted by score, highest first)
    checkpoint_sorted = sorted(checkpoint_strategy.items(), key=lambda x: x[1], reverse=True)[:top_n]
    checkpoint_top = [netuid for netuid, score in checkpoint_sorted]
    
    # Get top N subnets from current (sorted by score, highest first)
    current_sorted = sorted(current_strategy.items(), key=lambda x: x[1], reverse=True)[:top_n]
    current_top = [netuid for netuid, score in current_sorted]
    
    # Points for each position: 1st gets top_n points, last gets 1 point
    total_penalty = 0
    max_penalty = 0
    
    for old_position, netuid in enumerate(current_top):
        # Points for this subnet based on its old position
        points = top_n - old_position
        
        # Add to maximum possible penalty
        max_penalty += points * top_n
        
        if netuid not in checkpoint_top:
            # Subnet is gone completely
            total_penalty += points * top_n
        else:
            # Subnet still exists, find its new position
            new_position = checkpoint_top.index(netuid)
            
            if new_position > old_position:
                # Subnet fell down
                positions_lost = new_position - old_position
                total_penalty += points * positions_lost
            # If subnet moved up, no penalty
    
    # Calculate score (0 to 1)
    if max_penalty == 0:
        return 0.0
    
    score = total_penalty / max_penalty
    return score

def calculate_difference_score(past_generate_time, checkpoint, current_strategy, checkpoint_strategy):
    time_delta = (checkpoint - past_generate_time).total_seconds() / 3600.0
    progress_score = min(1.0, (0.5 + 0.07 * time_delta / 24))
    absolute_difference = calculate_absolute(current_strategy, checkpoint_strategy)
    absolute_score = calculate_absolute_score(absolute_difference)
    top15_score = calculate_top15_score(current_strategy, checkpoint_strategy)
    final_score = progress_score * (0.6 * top15_score + 0.4 * absolute_score)
    return final_score
