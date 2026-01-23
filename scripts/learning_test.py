# 老鱼简历的技能等级评分
skills = ["Python", "Java", "JavaScript", "MySQL", "Redis"]

# 创建技能长度列表
skill_lengths = [len(skill) for skill in skills]
print(f"技能名称长度: {skill_lengths}")

# 创建技能评分（基于长度的简单评分）
skill_scores = [len(skill) * 10 for skill in skills]
print(f"技能评分: {skill_scores}")

# 处理字符串
course_name = "Python Web Development"
uppercase_chars = [char.upper() for char in course_name if char.isalpha()]
print(f"大写字母: {uppercase_chars}")


# 程序员鱼皮的项目统计
project_stats = ("编程导航", 1000, 50000, 4.8)

# 元组拆包
project_name, issues, users, rating = project_stats
print(f"项目: {project_name}")
print(f"问题数: {issues}")
print(f"用户数: {users}")
print(f"评分: {rating}")

# 交换变量值
a, b = "Python", "Java"
print(f"交换前: a={a}, b={b}")
a, b = b, a
print(f"交换后: a={a}, b={b}")

# 函数返回多个值
def get_user_stats():
    return "程序员鱼皮", 100, 4.9

username, articles, score = get_user_stats()
print(f"用户: {username}, 文章: {articles}, 评分: {score}")
