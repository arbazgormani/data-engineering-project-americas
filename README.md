# **Crime and Unemployment Analysis in the United States**

## **Methods of Advanced Data Engineering Project**

### **Description**
This project investigates the relationship between unemployment rates and crime incidents across U.S. states. By integrating two datasets, one containing crime statistics and the other capturing unemployment rates. This analysis explores correlations between economic instability and criminal activity.

## **Features**

- **ETL Pipeline**: Extracts, transforms, and loads data from data sources.
- **Exploratory Data Analysis (EDA)**:
  - Crime trends across states.
  - Yearly crime and unemployment trends.
  - Regional crime variations.
  - Correlation between unemployment rates and crime incidents.
- **Summary**
- States with larger populations (e.g., Florida, California, Texas) report higher crime incidents.
- Crime incidents tend to rise in periods of economic instability but show no strong correlation with unemployment rates.
- A heatmap analysis reveals a weak correlation between unemployment and crime rates, suggesting other socio-economic factors play a critical role.

## **Tools & Technologies**
- **Python** (Pandas, NumPy, Matplotlib, Seaborn)

## **Installation**

1. Clone the repository:
   ```sh
   git clone https://github.com/arbazgormani/data-engineering-project-americas.git
   cd data-engineering-project-americas
   ```
2. Create and activate a virtual environment (optional but recommended):
   ```sh
   python -m venv venv
   source venv/bin/activate  # On Windows use: venv\Scripts\activate
   ```
3. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

## **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.












# Methods of Advanced Data Engineering Template Project

This template project provides some structure for your open data project in the MADE module at FAU.
This repository contains (a) a data science project that is developed by the student over the course of the semester, and (b) the exercises that are submitted over the course of the semester.

To get started, please follow these steps:
1. Create your own fork of this repository. Feel free to rename the repository right after creation, before you let the teaching instructors know your repository URL. **Do not rename the repository during the semester**.

## Project Work
Your data engineering project will run alongside lectures during the semester. We will ask you to regularly submit project work as milestones, so you can reasonably pace your work. All project work submissions **must** be placed in the `project` folder.

### Exporting a Jupyter Notebook
Jupyter Notebooks can be exported using `nbconvert` (`pip install nbconvert`). For example, to export the example notebook to HTML: `jupyter nbconvert --to html examples/final-report-example.ipynb --embed-images --output final-report.html`


## Exercises
During the semester you will need to complete exercises using [Jayvee](https://github.com/jvalue/jayvee). You **must** place your submission in the `exercises` folder in your repository and name them according to their number from one to five: `exercise<number from 1-5>.jv`.

In regular intervals, exercises will be given as homework to complete during the semester. Details and deadlines will be discussed in the lecture, also see the [course schedule](https://made.uni1.de/).

### Exercise Feedback
We provide automated exercise feedback using a GitHub action (that is defined in `.github/workflows/exercise-feedback.yml`). 

To view your exercise feedback, navigate to Actions → Exercise Feedback in your repository.

The exercise feedback is executed whenever you make a change in files in the `exercise` folder and push your local changes to the repository on GitHub. To see the feedback, open the latest GitHub Action run, open the `exercise-feedback` job and `Exercise Feedback` step. You should see command line output that contains output like this:

```sh
Found exercises/exercise1.jv, executing model...
Found output file airports.sqlite, grading...
Grading Exercise 1
	Overall points 17 of 17
	---
	By category:
		Shape: 4 of 4
		Types: 13 of 13
```
