# Fundamentals of Data Engineering – Exercise 2 Correction
### Student: Carla Domenech

---

## Correction Notes

### Issues to solve:

#### (scrapper) Modify get_songs to use catalog (2 points)
- [ ] Points awarded: 2
- [ ] Comments:

#### (scrapper) Check logs for strange messages (0.5 points)
- [ ] Points awarded: 0.3
- [ ] Comments: You should have checked the logs when the catalog is downloading, where the main issue is.

#### (cleaner) Avoid processing catalogs (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments:

#### (Validator) Fix directory creation issue (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments:

#### (Validator) Additional validation rule (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments: Rule is OK, and you keep aplying the older one, good job. You should have, maybe, add the rule on a different function so it is clearer and reusable.

#### Code improvements (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments:

### Functionalities to add:

#### 'results' module (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments:

#### 'lyrics' module (2 points)
- [ ] Points awarded: 1
- [ ] Comments: Good Job on the lyric extraction. But you are storing the results in the same validation folder. *This is a critical failure*. You should have created a new directory called 'lyrics' inside the files directory so you can access the lyrics only easily.

#### 'insights' module (2 points)
- [ ] Points awarded: 2
- [ ] Comments: Good job here.

#### Main execution file (1 point)
- [ ] Points awarded: 1
- [ ] Comments: Good job. 

---

## Total Score: 8.8 / 10 points

## General Comments:

Excellent work, Carla! You've successfully implemented almost all the required functionalities with a high degree of quality. Your code is generally well-structured and functional, demonstrating a solid understanding of the data pipeline architecture.

**Strengths:**
- Clean implementation of the scrapper module with proper catalog usage
- Good validation rule that maintains both old and new rules
- Excellent insights module - works perfectly and provides valuable analysis
- Proper main execution file using subprocess library
- All core modules are functional and deliver results

**Area for Improvement:**
The main issue is the **critical failure** in the lyrics module where results are stored in the validation folder instead of a dedicated lyrics directory. This organizational issue affects data accessibility and pipeline clarity. While this is marked as critical, the actual extraction logic works correctly, which is why you received partial credit.

**Recommendations:**
- Always create dedicated output directories for each processing stage
- Consider adding logging to all modules for better monitoring and debugging
- Document your architectural decisions, especially regarding data flow and storage

Overall, this is a strong submission that shows you understand the fundamentals of data engineering pipelines. With attention to output organization and logging practices, your next project will be even better. Great job!
