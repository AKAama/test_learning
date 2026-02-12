#
# 给你一个字符串数组，请你将
# 字母异位词
# 组合在一起。可以按任意顺序返回结果列表。
#
#
#
# 示例
# 1:
#
# 输入: strs = ["eat", "tea", "tan", "ate", "nat", "bat"]
#
# 输出: [["bat"], ["nat", "tan"], ["ate", "eat", "tea"]]
#
# 解释：
#
# 在strs中没有字符串可以通过重新排列来形成 "bat"。
# 字符串"nat"和 "tan"是字母异位词，因为它们可以重新排列以形成彼此。
# 字符串"ate" ，"eat"和"tea"是字母异位词，因为它们可以重新排列以形成彼此。
#
# 提示：
#
# 1 <= strs.length <= 104
# 0 <= strs[i].length <= 100
# strs[i]
# 仅包含小写字母
from collections import defaultdict
from typing import List

from duckdb.experimental.spark.sql.functions import length


#
# def groupAnagrams(strs: List[str]) -> List[List[str]]:
#     anagrams = defaultdict(list)
#
#     for s in strs:
#         key = "".join(sorted(s))
#         anagrams[key].append(s)
#     return list(anagrams.values())
#
# strs = ["eat", "tea", "tan", "ate", "nat", "bat"]
# groupAnagrams(strs)

#
# 输入：nums = [100,4,200,1,3,2]
# 输出：4
# 解释：最长数字连续序列是 [1, 2, 3, 4]。它的长度为 4。


# 输入: nums = [0,1,0,3,12]
# 输出: [1,3,12,0,0]
def moveZeroes(self, nums: List[int]) -> None:
    count = 0
    for num in nums:
        if num == 0:
            count+=1
        num

nums = [0, 1, 0, 3, 12]
moveZeroes(nums)