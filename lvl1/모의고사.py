# There are three students who has their own specific answer pattern.
# The questions will be provided and the amount of questions are 10,000 at most. 
# The answer list should contain student's number who got the highest score.
# If there are two and more students who have the highest score, the answer list sould contains all of the student's number in order.

def solution(answers):
    answer = []
    def student(answers, pattern):
        num = 0
        correct_num = 0
        for i in range(len(answers)):
            if i != 0 and i % len(pattern) == 0:
                num += len(pattern)
            if answers[i] == pattern[i-num]:
                correct_num += 1
        return correct_num
    def grade(*scores):
        total_lst = []
        for i in scores:
            total_lst.append(i)
        max_score = max(total_lst)
        for i in range(len(scores)):
            if scores[i] == max_score:
                answer.append(i+1)
        
    st1_pattern = [1,2,3,4,5]
    st2_pattern = [2, 1, 2, 3, 2, 4, 2, 5]
    st3_pattern = [3, 3, 1, 1, 2, 2, 4, 4, 5, 5]
    s1 = student(answers, st1_pattern)    
    s2 = student(answers, st2_pattern)
    s3 = student(answers, st3_pattern)
    grade(s1, s2, s3)

    return answer
