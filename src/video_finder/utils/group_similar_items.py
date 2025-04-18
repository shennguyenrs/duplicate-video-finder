import itertools
import logging
from collections import defaultdict


def group_similar_items(item_pairs_with_similarity):
    """
    Groups items based on similarity pairs and calculates average similarity for each group.

    Args:
        item_pairs_with_similarity (list): A list of tuples, where each tuple represents
                           a pair of similar items and their similarity score
                           (e.g., [(item1, item2, 95.5), (item2, item3, 98.0)]).

    Returns:
        list: A list of tuples `(group_set, average_similarity)`, where each `group_set`
              contains grouped similar items and `average_similarity` is the mean
              similarity of pairs *within* that group.
    """
    if not item_pairs_with_similarity:
        return []

    logging.info("Grouping similar items and calculating average similarity...")
    adj = defaultdict(list)
    all_items = set()
    # Store similarities for easy lookup, using sorted tuples as keys
    pair_similarities = {}
    for u, v, similarity in item_pairs_with_similarity:
        adj[u].append(v)
        adj[v].append(u)
        all_items.add(u)
        all_items.add(v)
        # Ensure consistent key order for lookup
        pair_key = tuple(sorted((u, v)))
        pair_similarities[pair_key] = similarity

    groups_with_similarity = []
    visited = set()

    for item in all_items:
        if item not in visited:
            current_group = set()
            # Use a list as a queue for BFS
            q = [item]
            visited.add(item)
            while q:
                curr = q.pop(0)
                current_group.add(curr)
                for neighbor in adj[curr]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        q.append(neighbor)

            # Only process groups with more than one item (actual duplicates/similars)
            if len(current_group) > 1:
                group_similarity_sum = 0.0
                group_pair_count = 0
                # Iterate through all unique pairs within the found group
                for item_a, item_b in itertools.combinations(current_group, 2):
                    pair_key = tuple(sorted((item_a, item_b)))
                    # Check if this pair was one of the original similar pairs
                    if pair_key in pair_similarities:
                        group_similarity_sum += pair_similarities[pair_key]
                        group_pair_count += 1

                average_similarity = 0.0
                if group_pair_count > 0:
                    average_similarity = group_similarity_sum / group_pair_count
                else:
                    # This case should ideally not happen for groups > 1 derived
                    # from pairs, but handle defensively. Could happen if a group
                    # is formed transitively but no direct pairs within it met the threshold.
                    logging.warning(
                        f"Group {current_group} has no internal pairs meeting threshold? Assigning 0 similarity."
                    )

                groups_with_similarity.append((current_group, average_similarity))

    logging.info(f"Found {len(groups_with_similarity)} groups of similar items.")
    # Sorting is now done in core.py after this function returns
    return groups_with_similarity
