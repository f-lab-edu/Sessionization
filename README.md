# Sessionization
이 프로젝트는 웹사이트에서 발생하는 사용자의 활동을 세션 단위로 그룹화하는 것을 목적으로 진행하였습니다.

## Clickstream Data
일반적으로 클릭스트림 데이터는 사용자의 다양한 행동을 추적하지만, 이 프로젝트에서는 구매 이벤트만 포함된 데이터를 사용합니다. 

출처 : https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

원본 데이터는 사용자 세션 정보를 포함하지만, 이 프로젝트에서는 사용자 세션을 직접 생성하는 것을 목표로 했기 때문에 이를 제외하고 진행하였습니다.

## 기대효과
사용자가 사이트를 방문하여 페이지를 탐색하거나 다양한 이벤트를 발생시키는 일련의 활동을 하나의 세션으로 묶음으로써, 사용자 행동을 보다 명확하게 분석하는 것을 기대할 수 있습니다.


# Implementation
- 1시간 단위로 배치 작업
- Spark를 활용한 데이터 처리
- 세션 종료 조건: 마지막 활동으로부터 30분 이상 경과한 시점
- pytest를 활용한 TDD(Test Driven Development)